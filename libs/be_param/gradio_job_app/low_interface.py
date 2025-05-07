import sys
sys.path.append('.')

import requests
import random
import time
import spacy
import ast
from typing import Dict, Any, Optional
from pydantic import BaseModel
from libs.be_param.gradio_job_app.prompts import PROMPTS

class RispostaLLM(BaseModel):
    keywords: list[str]

class MiniKG(BaseModel):
    triple_list: list[list[str, str, str]]

# Prompts


class LowLevelAPI:
    """
    Low-level API for interacting with LLM services and NLP tools.
    Provides methods for text processing, keyword extraction, and vector generation.
    """
    local_embed_url = "http://debianInference.local.lan:11434//api/embeddings"
    local_llm_url = "http://debianInference.local.lan:11434/api/generate"
    model_list = ["phi4", "qwen2.5:0.5b", "llama3.2:1b", 'gemma3:1b']

    def _configure_lang(self, lang: str) -> None:
        """Configure language-specific settings and models"""
        self.lang = lang
        if lang == "ita":
            self.prompt, self.formatted_prompt_triple = PROMPTS["ita"], PROMPTS["ita"]["triple"]
        else:
            self.prompt, self.formatted_prompt_triple = PROMPTS["eng"], PROMPTS["eng"]["triple"]
        self.spacy_nlp = spacy.load('it_core_news_md' if lang == "ita" else 'en_core_web_md')
        self.formatted_prompt_kw = self.prompt['keywords']

    def __configure_model(self, model: str) -> None:
        """Configure the LLM model to use"""
        self.model = model if model in self.model_list else random.choice(self.model_list)
        self.LLM_uri = self.local_llm_url

    def __init__(self, model: str = "phi4", lang: str = "ita") -> None:
        """
        Initialize the API with specified model and language.
        
        Args:
            model: The LLM model to use (default: "phi4")
            lang: Language code, either "ita" or "eng" (default: "ita")
        """
        self.__configure_model(model)
        self._configure_lang(lang)

    def _call_LLM(self, user_prompt: str, system_prompt: str = "You are a helpful assistant",
                  output_format: str = "text", schema: Any = None) -> str:
        """
        Call the LLM with the given prompts and parameters.
        
        Args:
            user_prompt: The prompt to send to the LLM
            system_prompt: System instructions for the LLM
            output_format: Format for the output ("text" or "json")
            schema: JSON schema for structured output
            
        Returns:
            The LLM response as a string
        """
        common_setup = {"model": self.model, "stream": False}
        headers = None
        payload = {
            **common_setup, 
            "keep_alive": 10, 
            "system": system_prompt, 
            "prompt": user_prompt,
            "format": schema() if output_format == 'json' else "json"
        }
        response = requests.post(self.LLM_uri, headers=headers, json=payload)
        if response.status_code != 200:
            return f"Errore: {response.status_code} - {response.text}"
        json_response = response.json()
        return json_response["response"]

    def _generate_doc2vector(self, text: str, lang: str = 'ita') -> list:
        """
        Generate document vector using spaCy.
        
        Args:
            text: Input text to vectorize
            lang: Language code
            
        Returns:
            Vector representation of the text
        """
        if self.lang != lang:
            self._configure_lang(lang)
        doc = self.spacy_nlp(text)
        return doc.vector.tolist()

    def _doc2vectorLLM(self, text: str) -> list:
        """
        Generate document vector using LLM.
        
        Args:
            text: Input text to vectorize
            
        Returns:
            Vector representation of the text
        """
        self.__configure_model('phi4')
        data = {
            "model": "phi4",  # best embedding model
            "prompt": text
        }
        response = requests.post(self.local_embed_url, json=data)
        if response.status_code == 200:
            embedding = response.json()
            return embedding['embedding']
        else:
            return []

    def _extract_keywords(self, text: str, method: str = 'mini_LLM', lang: str = 'ita') -> list:
        """
        Extract keywords from text.
        
        Args:
            text: Input text to extract keywords from
            method: Method to use for extraction
            lang: Language code
            
        Returns:
            List of extracted keywords
        """
        if self.lang != lang:
            self._configure_lang(lang)
            self.__configure_model('phi4')
        match method:
            case "mini_LLM":
                time.sleep(0.7)
                prompt = self.formatted_prompt_kw.format(text=text)
                response = self._call_LLM(prompt, output_format="json")
                try:
                    return ast.literal_eval(str(response))['keywords']
                except:
                    return []
            case _:
                return []


