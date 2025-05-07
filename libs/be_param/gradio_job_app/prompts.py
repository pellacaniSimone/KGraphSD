
PROMPTS = {
    "ita": {
        "keywords": "Ritorna sole parole chiave brevi e concise dal seguente testo.\n------\n TESTO: {text}",
        "triple": {
            "system_prompt": """
                Sei un assistente specializzato nell'estrazione di triple RDF da un testo.  
                Le triple sono strutturate nella forma (soggetto, predicato, oggetto) e rappresentano fatti concisi.  
                Le entità devono essere identificate chiaramente, e i predicati devono esprimere relazioni semantiche tra di esse.  
                Evita informazioni ridondanti o interpretazioni soggettive.  
                Restituisci solo triple ben formate, senza altre spiegazioni o testo aggiuntivo.  
            """,
            "user_prompt": """
            ---------------------------------------------------------------
            Ecco un testo da cui estrarre triple RDF:  
            "{text}"  
            ---------------------------------------------------------------
            Usa le seguenti keyword per aiutarti a identificare relazioni e concetti chiave: {keywords}  
            ---------------------------------------------------------------
            Restituisci le triple nel formato (soggetto, predicato, oggetto). 
            """
        }
    },
    "eng": {
        "keywords": "RETURN ONLY BRIEF AND CONCISE KEYWORDS FROM THE FOLLOWING TEXT.\n------\n TESTO: {text}",
        "triple": {
            "system_prompt": """
                You are an assistant specialized in extracting RDF triples from text.  
                Triples follow the structure (subject, predicate, object) and represent concise facts.  
                Entities must be clearly identified, and predicates should express semantic relationships between them.  
                Avoid redundant information or subjective interpretations.  
                Return only well-formed triples without any additional explanation or extra text.
            """,
            "user_prompt": """
                Here is a text from which to extract RDF triples:  
                "{text}"  

                Use the following keywords to help identify key concepts and relationships: {keywords}  

                Return the triples in the format (subject, predicate, object).
            """
        }
    }
}

