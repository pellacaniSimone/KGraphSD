from typing import Any, Dict, List, Tuple
from psycopg2.extras import Json
import psycopg2
import hashlib

DEBUG = True

class DatabaseHandler:
    """
    Handles database operations for the Job Catalog application.
    
    Provides methods for:
    - Creating database schema and tables
    - Inserting vertices and edges in the graph
    - Managing SQL and AGE (Apache Graph Extension) operations
    """
    
    def _preprocess_query(self, qt: str, dparams: Dict[str | int, Any] = None) -> str:
        """
        Preprocess SQL query by formatting with parameters.
        
        Args:
            qt: Query template
            dparams: Additional parameters for formatting
            
        Returns:
            Formatted query string
        """
        if DEBUG:
            print(f"[DEBUG] Preprocessing query with params: {dparams}")
        kparam = dict(eval(self.Const.KPARAM))
        eparam = {**kparam, **dparams} if dparams else kparam
        return qt.format(**eparam)

    def __execute(self, *args) -> None:
        """
        Execute SQL query.
        
        Args:
            *args: Query and parameters
        """
        if DEBUG:
            print(f"[DEBUG] Executing query: {args[0][:100]}...")
        with self.conn.cursor() as cursor:
            cursor.execute(*args)

    def __execute_query_list(self, ql: List[str]) -> None:
        """
        Execute a list of SQL queries.
        
        Args:
            ql: List of query templates
        """
        for query in ql:
            self.__execute(self._preprocess_query(query))

    def _execute_fetch(self, *args) -> list:
        """
        Execute SQL query and fetch results.
        
        Args:
            *args: Query and parameters
            
        Returns:
            Query results
        """
        with self.conn.cursor() as cursor:
            cursor.execute(*args)
            return cursor.fetchall()

    def _insert_sql_vertex(self, params: Dict[str, Any]) -> None:
        """
        Insert vertex data into SQL table.
        
        Args:
            params: Vertex data
        """
        if DEBUG:
            print(f"[DEBUG] Inserting SQL vertex with type: {params.get('type', 'unknown')}")
        keys = ', '.join(params.keys())
        placeholders = ', '.join(['%s'] * len(params))
        sql_q = self._preprocess_query(self.Const.INSERT_SQL_RECORD, {'keys': keys, 'value': placeholders})
        param_values = tuple(Json(v) if isinstance(v, dict) else v for v in params.values())
        self.__execute(sql_q, param_values)
        self.conn.commit()
        if DEBUG:
            print("[DEBUG] SQL vertex inserted and committed")

    def _insert_age_record_vertex(self, tuid: str, entity: str, sink: bool = True) -> None:
        """
        Insert vertex into AGE graph.
        
        Args:
            tuid: Unique identifier
            entity: Entity name
            sink: Whether this is a sink node
        """
        if DEBUG:
            print(f"[DEBUG] Inserting AGE vertex: {entity} (tuid: {tuid[:8]}...)")
        entity = entity if entity else "unknown_entity"
        
        age_q = self._preprocess_query(self.Const.INSERT_AGE_VERTEX, 
                                      {'tuid': tuid, 'the_relavent_entity': entity})
        self.__execute_query_list(self.Const.AGE_SETUP_ENV)
        self.__execute(age_q)
        self.conn.commit()
        if DEBUG:
            print("[DEBUG] AGE vertex inserted and committed")

    def _insert_age_record_edge(self, source_id: str, relation: str, target_id: str) -> None:
        """
        Insert edge into AGE graph.
        
        Args:
            source_id: Source vertex ID
            relation: Edge relation type
            target_id: Target vertex ID
        """
        if DEBUG:
            print(f"[DEBUG] Inserting AGE edge: {source_id[:8]}... --[{relation}]--> {target_id[:8]}...")
        relation = relation if relation and relation.strip() else "LINKS_TO"
        
        par = {
            'source_id': source_id,
            'rel_type': relation,
            'target_id': target_id
        }
        
        t1_sql = self._preprocess_query(self.Const.INSERT_AGE_EDGE, par)
        self.__execute_query_list(self.Const.AGE_SETUP_ENV)
        self.__execute(t1_sql)
        self.conn.commit()
        if DEBUG:
            print("[DEBUG] AGE edge inserted and committed")

    def perform_vertex_insert(self, params: Dict[str | int, Any]) -> None:
        """
        Insert vertex data into both SQL and AGE.
        
        Args:
            params: Vertex data
        """
        _ = self.RecordData(**params)
        self._insert_sql_vertex(params)
        self._insert_age_record_vertex(params['tuid'], params['type'])

    def perform_edge_insert(self, source_entity: str, relation: str, target_entity: str, document_tuid: str) -> None:
        """
        Insert edge data into AGE.
        
        Args:
            source_entity: Source entity name
            relation: Relation type
            target_entity: Target entity name
            document_tuid: Document ID
        """
        if DEBUG:
            print(f"[DEBUG] Performing edge insert: {source_entity} --[{relation}]--> {target_entity}")
        if not relation or not relation.strip():
            relation = "LINKS_TO"
        
        source_id = hashlib.sha224((source_entity + document_tuid).encode()).hexdigest()
        target_id = hashlib.sha224((target_entity + document_tuid).encode()).hexdigest()
        
        self._insert_age_record_vertex(source_id, source_entity, sink=False)
        self._insert_age_record_vertex(target_id, target_entity, sink=False)
        
        self._insert_age_record_edge(source_id, relation, target_id)
        if DEBUG:
            print("[DEBUG] Edge insertion complete")

    def __init__(self, Const=None, RecordValidator=None, TripleValidator=None) -> None:
        """
        Initialize database handler.
        
        Args:
            Const: Constants class
            RecordValidator: Record data validator
            TripleValidator: Triple data validator
        """
        if DEBUG:
            print("[DEBUG] Initializing DatabaseHandler")
        self.Const = Const()
        self.RecordData = RecordValidator
        self.TripleData = TripleValidator
        self.conn = psycopg2.connect(**self.Const.DB_CONFIG)
        if not self.conn:
            if DEBUG:
                print("[DEBUG] Failed to establish database connection")
            raise ConnectionError("Failed to establish database connection.")
        else:
            if DEBUG:
                print("[DEBUG] Database connection established")
            __fetched = self._execute_fetch(self._preprocess_query(self.Const.CK_CREATION_Q))
        if not any(self.Const.SCHEMA_NAME in row for row in __fetched):
            if DEBUG:
                print(f"[DEBUG] Schema {self.Const.SCHEMA_NAME} not found, creating...")
            self.__execute_query_list(self.Const.CREATION_QUERY_LIST)
            self.conn.commit()
            if DEBUG:
                print(f"[DEBUG] Schema {self.Const.SCHEMA_NAME} created")

    def close(self) -> None:
        """
        Close database connection.
        """
        if self.conn:
            self.conn.close()
        if DEBUG:
            print("[DEBUG] Database connection closed")





