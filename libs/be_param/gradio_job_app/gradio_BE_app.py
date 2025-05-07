import os
os.environ['PROD'] = 'False'  # permit production on different instance

# to include the Repo code without installing in the environment
import sys
sys.path.append('.')

import uuid, random, hashlib, json, re, ast
from psycopg2.extras import Json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Final
from dataclasses import dataclass, field
from abc import ABC
from libs.pg_engine.database_pg_connector import DatabaseHandler
from libs.be_param.gradio_job_app.low_interface import LowLevelAPI, MiniKG
from pydantic import BaseModel

"""
Job Catalog Application Backend
===============================

This module implements the backend logic for the Job Catalog application, which processes
job offers and stores them in a multi-paradigm database (PostgreSQL with vector, graph, and
relational capabilities).

The module follows an MVC-like pattern:
- Model: RecordData, TripleData, Const classes define data structures
- Controller: JobCatalogApp processes inputs and manages data flow
- View: Handled by the Gradio frontend in app_gradio.py

The application extracts keywords, generates vector embeddings, and creates knowledge graph
triples from job offer descriptions, storing all this information in the database.
"""

DEBUG = True

class DataGenerator:
    def __init__(self, model: str = "phi4", lang="ita"):
        """
        Initialize the data generator with specified model and language.
        
        Args:
            model: The LLM model to use (default: "phi4")
            lang: Language code, either "ita" or "eng" (default: "ita")
        """
        self.API = LowLevelAPI(model, lang)
        self.model, self.lang = model, lang

    def attention_keyword_generator(self, text: str) -> list:
        """
        Extract keywords from text using LLM. Need at least qwen 0.5b
        Args:
          text: Input text to extract keywords from

        Returns:
          List of extracted keywords
        """
        if DEBUG:
            print(f"[DEBUG] Extracting keywords from text")
        try:
            return self.API._extract_keywords(text)
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Error extracting keywords: {e}")
            return ["job", "offer", "position"]  # Fallback keywords

    def doc2vector_generator(self, keywords_alphabetical_order: str) -> list:
        """
        Generate document vector from keywords using spaCy.
        Non parametric algo, return shape 300, text is the keywords, using spaCy.
        
        Args:
          keywords_alphabetical_order: Space-separated keywords in alphabetical order
            
        Returns:
          Vector representation (300 dimensions) of the keywords
        """
        if DEBUG:
            print(f"[DEBUG] Generating vector from keywords")
        try:
            return self.API._generate_doc2vector(keywords_alphabetical_order, self.lang)
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Error generating keyword vector: {e}")
            return [random.uniform(-1, 1) for _ in range(300)]  # Fallback vector

    def attention_doc2vector_generator(self, text: str) -> list:
        """
        Generate document vector using LLM.
        Vectorize document via LLM best shape (5120) is phi4 
        
        Args:
            text: Input text to vectorize
            
        Returns:
            Vector representation (5120 dimensions) of the text
        """
        if DEBUG:
            print(f"[DEBUG] Generating attention vector from text")
        try:
            result = self.API._doc2vectorLLM(text)
            if result:
                return result
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Error generating attention vector: {e}")
        
        # Fallback: generate random vector
        return [random.uniform(-1, 1) for _ in range(5120)]

    @staticmethod
    def uuid_generator(val=None) -> str:
        """
        Generate a unique identifier.
        
        Args:
          val: Optional entity name to include in hash generation
            
        Returns:
          SHA-224 hash as string
        """
        app = val if val else str(datetime.now(tz=timezone.utc)) + "|" + str(uuid.uuid4())
        app = hashlib.sha224(app.encode())
        app = app.hexdigest()
        return str(app)

    def triple_generate(self, text: str, keywords: list) -> str:
        """
        Generate knowledge graph triples from text and keywords using LLM.
        
        Args:
          text: Input text to analyze
          keywords: Keywords extracted from the text
            
        Returns:
          JSON string containing triples in subject-predicate-object format
        """
        if DEBUG:
            print(f"[DEBUG] Generating triples from text and keywords")
        try:
            user_prompt = self.API.formatted_prompt_triple['user_prompt'].format(
                text=text, 
                keywords=", ".join(keywords) if isinstance(keywords, list) else keywords
            )
            system_prompt = self.API.formatted_prompt_triple['system_prompt']
            return self.API._call_LLM(
                user_prompt=user_prompt,
                system_prompt=system_prompt,
                output_format='json',
                schema=MiniKG.model_json_schema
            )
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Error generating triples: {e}")
            # Return a minimal valid triple structure
            return '{"triple_list": [["job", "requires", "skills"], ["company", "offers", "position"]]}'

@dataclass
class TripleData:
    """
    Data class for validating knowledge graph triples.
    
    Structure:
    - triple: ((entity1, tuid1), relation, (entity2, tuid2))
    """
    triple: Tuple[Tuple[str, str], str, Tuple[str, str]]


@dataclass
class RecordData:
    """
    Data class for job offer records stored in the database.
    
    Fields:
    - type: Application name or job platform
    - title: Entity title (job title)
    - data: Dictionary containing job details
    - att_vector_size: Attention vector (5120 dimensions)
    - keyword2vector: Keyword vector (300 dimensions)
    - time: Timestamp (UTC)
    - tuid: Unique identifier
    """
    type: str   # name of the app 
    title: str  # name of the entity title
    data: Dict[str, any] 
    att_vector_size: List[float] 
    keyword2vector: List[float] 
    time: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    tuid: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class Const(ABC):
    """
    Constants and configuration for the application.
    
    This class defines database connection parameters, schema names,
    vector dimensions, and SQL queries for database operations.
    """
    PROD: Final[bool] = field(default=os.getenv('PROD', 'True') == 'True')
    DB_CONFIG: Final[Dict[str, Any]] = field(default_factory=lambda: {
        "dbname": "db_multimodale_multistruttura", 
        "user": "postgres",
        "password": "",  # password here
        "host": "debianMultiModelDB.local.lan",
        "port": "5432",
    })
    SCHEMA_NAME: Final[str] = field(default="test_schema")
    APP_NAME: Final[str] = field(default="test_app_table")
    HT_NAME: Final[str] = field(default="test_nodes_tab")
    W2VECTOR_SIZE: Final[int] = field(default=300)
    ATT_VECTOR_SIZE: Final[int] = field(default=5120)
    # sql creation
    CREATION_QUERY_LIST: Final[list[str]] = field(default_factory=lambda: [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "CREATE EXTENSION IF NOT EXISTS age;",
        "CREATE EXTENSION IF NOT EXISTS timescaledb;",
        "CREATE EXTENSION IF NOT EXISTS plpython3u;",
        "LOAD 'age';",
        "SET search_path = ag_catalog, \"$user\", public;",
        "SELECT * FROM ag_catalog.create_graph('{schema_name}');",
        """
        CREATE TABLE {schema_name}.{ht_name} (
            type TEXT, 
            title TEXT,
            sink BOOLEAN NOT NULL DEFAULT TRUE,
            att_vector_size vector({att_vector_size}),
            keyword2vector vector({w2vector_size}),
            data JSONB,
            tuid TEXT,
            time TIMESTAMP NOT NULL,
            PRIMARY KEY (time, tuid) 
        );
        """,
        "SELECT create_hypertable('{schema_name}.{ht_name}', 'time');"
    ])
    
    # sql interact MOD HERE
    KPARAM: Final[str] = field(default='''{"schema_name":self.Const.SCHEMA_NAME,"ht_name":self.Const.HT_NAME, "att_vector_size":self.Const.ATT_VECTOR_SIZE, "w2vector_size":self.Const.W2VECTOR_SIZE }''')
    CK_CREATION_Q: Final[str] = field(default="SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
    INSERT_SQL_RECORD: Final[str] = field(default=""" INSERT INTO {schema_name}.{ht_name} ({keys}) VALUES ({value}) """)

    # AGE interact
    AGE_SETUP_ENV: Final[list[str]] = field(default_factory=lambda: [
        "CREATE EXTENSION IF NOT EXISTS age;",
        "LOAD 'age';",
        "SET search_path = ag_catalog, \"$user\", public;",
    ])

    INSERT_AGE_VERTEX: Final[str] = field(default="""
        SELECT * FROM cypher('{schema_name}', $$
            CREATE (:Node {{id: '{tuid}', entity: '{the_relavent_entity}'}} )
        $$ ) AS (result agtype);
    """)

    INSERT_AGE_EDGE: Final[str] = field(default="""
        SELECT * FROM cypher('{schema_name}', $$
        MATCH (a:Node {{id: '{source_id}'}}), (b:Node {{id: '{target_id}'}})
        CREATE (a)-[:LINKS_TO {{label: '{rel_type}'}}]->(b)
        $$) AS (result agtype);
    """)


class JobCatalogApp:
    def __clean_text(self, text: str):
        """
        Clean text by removing non-ASCII characters and special characters
        Args:
            text: Input text to clean
        Returns:
            Cleaned text
        """
        if DEBUG:
            print(f"[DEBUG] Cleaning text")
        cleaned_text = text.encode('ascii', 'ignore').decode('ascii')
        pattern = r"[^a-zA-Z0-9]"
        cleaned_text = re.sub(pattern, ' ', cleaned_text).strip()
        return cleaned_text

    def __pattern_preprocess(self, s):
        """
        Replace initial digits with their Italian word equivalents
        """
        s = self.__clean_text(s)
        map_num = {
            '0': 'zero', '1': 'uno', '2': 'due', '3': 'tre', '4': 'quattro',
            '5': 'cinque', '6': 'sei', '7': 'sette', '8': 'otto', '9': 'nove'
        }
        match = re.match(r'^(\d+)(.*)', s)
        pattern = r"[ '{}():$,\-+]"
        s = re.sub(pattern, ' ', s)
        if match:
            digits_part, rest = match.groups()
            words = [map_num[d] for d in digits_part]  # map convert
            return ' '.join(words) + rest
        else:
            return s

    def __pp_backend(self, link: str, job_platform_input: str,
                    language_radio: str, offer_title: str, client_name: str,
                    position: str, presence: str,
                    relevant: str, full_input: str):
        """
        Process backend operations for job offer submission
        """
        if DEBUG:
            print("[DEBUG] Starting backend processing")
        
        generator = DataGenerator()
        cleaned_relevant = self.__clean_text(relevant)
        
        if DEBUG:
            print("[DEBUG] Extracting keywords")
        keywords = generator.attention_keyword_generator(cleaned_relevant)
        if DEBUG:
            print(f"[DEBUG] Extracted {len(keywords)} keywords")
        
        if DEBUG:
            print("[DEBUG] Generating document vector")
        att_vector = generator.attention_doc2vector_generator(full_input)
        
        generator = DataGenerator(lang=language_radio)
        cleaned_keywords = []
        for keyword in keywords:
            cleaned_keyword = self.__clean_text(keyword)
            if cleaned_keyword:
                cleaned_keywords.append(cleaned_keyword.lower())
        
        if DEBUG:
            print("[DEBUG] Generating keyword vector")
        kw_vector = generator.doc2vector_generator(" ".join(sorted(cleaned_keywords)))
        
        if DEBUG:
            print("[DEBUG] Generating triples")
        triple = generator.triple_generate(cleaned_relevant, cleaned_keywords)
        try:
            triple = dict(ast.literal_eval(triple))
        except (SyntaxError, ValueError):
            # Fallback if triple parsing fails
            triple = {"triple_list": [["job", "requires", "skills"], ["company", "offers", "position"]]}
        
        if DEBUG:
            print(f"[DEBUG] Generated {len(triple['triple_list'])} triples")

        job_platform_input = self.__clean_text(job_platform_input)
        cleaned_full_input = self.__clean_text(full_input)
        cleaned_position = self.__clean_text(position)
        cleaned_presence = self.__clean_text(presence)
        cleaned_offer_title = self.__clean_text(offer_title)
        cleaned_client_name = self.__clean_text(client_name)

        if DEBUG:
            print("[DEBUG] Preparing formatted data")
        formatted_data = {
            "type": job_platform_input,
            "title": f"{cleaned_offer_title} - {cleaned_client_name} - {cleaned_position} - {cleaned_presence}".strip(),
            "data": {
                "Link": str(link),
                "Lang": str(language_radio),
                "Detail": str(cleaned_full_input),
                "Relevant": str(cleaned_relevant),
                "Keywords": cleaned_keywords
            },
            "att_vector_size": att_vector,
            "keyword2vector": kw_vector,
            "time": datetime.now(tz=timezone.utc),
            "tuid": str(uuid.uuid4())
        }

        try:
            if DEBUG:
                print("[DEBUG] Initializing database handler")
            db_handler = DatabaseHandler(Const=Const, RecordValidator=RecordData, TripleValidator=TripleData)
            
            if DEBUG:
                print("[DEBUG] Inserting main vertex")
            db_handler.perform_vertex_insert(formatted_data)

            document_tuid = formatted_data["tuid"]
            if DEBUG:
                print(f"[DEBUG] Processing {len(triple['triple_list'])} triples for document {document_tuid[:8]}...")
            
            for i, t in enumerate(triple['triple_list']):
                if DEBUG:
                    print(f"[DEBUG] Processing triple {i+1}/{len(triple['triple_list'])}")
                source_entity = self.__pattern_preprocess(t[0]).replace(" ", "_")
                relation = self.__pattern_preprocess(t[1]).replace(" ", "_")
                target_entity = self.__pattern_preprocess(t[2]).replace(" ", "_")
                
                db_handler.perform_edge_insert(source_entity, relation, target_entity, document_tuid)
            
            db_handler.close()
            if DEBUG:
                print("[DEBUG] Backend processing complete")
            return {"insertion": "✅"}
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Database operation error: {e}")
            return {"insertion": "❌", "error": str(e)}

    def process_input(self, link, job_platform_input,
                      language_radio, offer_title, client_name, position, presence,
                      relevant, full_input):
        """Only error wrapper"""
        if DEBUG:
            print("[DEBUG] Processing input")
        try:
            result = self.__pp_backend(link, job_platform_input,
                                    language_radio, offer_title, client_name, position, presence,
                                    relevant, full_input)
            if DEBUG:
                print("[DEBUG] Input processing successful")
            return result
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] Error during processing: {e}")
            return {"error": str(e)}
