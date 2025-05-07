import os
os.environ['PROD'] = 'False'  # permit production on different instance

# to include the Repo code without installing in the environment
import sys
sys.path.append('.')

import uuid,random,os
from datetime import datetime, timezone
from typing import  Dict, List , Tuple, Any, Final
from dataclasses import dataclass,field
from abc import ABC
from libs.pg_engine.database_pg_connector import DatabaseHandler


def doc2vector_generator() -> list:
  """Used in test"""
  return [random.uniform(0, 1) for _ in range(Const.VECTOR_SIZE)]

@dataclass
class RecordData:
  type: str  # name of the app
  title: str # name of the entity title
  summary: str # brief description
  embedding: List[float] = field(default_factory=lambda: doc2vector_generator())  # Exactly 4 elements
  data: Dict[str, any] = field(default_factory=dict)
  time: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
  tuid: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class TripleData:
  """Provide triple validator"""
  triple:Tuple[Tuple[str,str],str,Tuple[str,str]] 


@dataclass
class Const(ABC):
  """Test field"""
  PROD: Final[bool] = field(default=os.getenv('PROD', 'True' ) == 'True')
  DB_CONFIG : Final[Dict[str, Any]] = field(default_factory=lambda:{ # ValueError: mutable default 
    "dbname": "db_multimodale_multistruttura" , 
    "user": "postgres",
    "password": "", # password here
    "host": "debianMultiModelDB.local.lan",
    "port": "5432"
  })
  SCHEMA_NAME:  Final[str] =field(default="test_schema")
  APP_NAME:  Final[str] = field(default="test_table")
  HT_NAME:  Final[str] = field(default="test_full_nodes_tab")
  TEST_EDGE_NAME_IS_TAB:  Final[str] = field(default='test_edge')
  VECTOR_SIZE: Final[int] = field(default=3)
  # sql creation
  CREATION_QUERY_LIST: Final[list[str]] =field(default_factory=lambda: [
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
            summary TEXT,
            leaf BOOLEAN NOT NULL DEFAULT TRUE,
            embedding vector({vector_size}),
            data JSONB,
            tuid TEXT,
            time TIMESTAMP NOT NULL,
            PRIMARY KEY (time, tuid) 
        );
        """,
        "SELECT create_hypertable('{schema_name}.{ht_name}', 'time');"
    ])
  # sql interact
  KPARAM:Final[str] = field(default  ='''{"schema_name":self.Const.SCHEMA_NAME,"ht_name":self.Const.HT_NAME, "vector_size":self.Const.VECTOR_SIZE }''' )
  CK_CREATION_Q :  Final[str] = field(default= "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
  INSERT_SQL_RECORD:  Final[str] = field(default= """ INSERT INTO {schema_name}.{ht_name} ({keys}) VALUES ({value}) """)

  # AGE interact
  AGE_SETUP_ENV :  Final[str] = field(default_factory=lambda: [
    "CREATE EXTENSION IF NOT EXISTS age;",
    "LOAD 'age';",
    "SET search_path = ag_catalog, '$user', public;",
  ] )
  INSERT_AGE_VERTEX :  Final[str] = field(default= """
    SELECT * FROM cypher('{schema_name}', $$
        CREATE (p:{the_relavent_entity} {{tuid:'{tuid}'}} )
    $$ ) AS (result agtype);
  """ )
  # rel_type is a table of similar edges
  INSERT_AGE_EDGE  :  Final[str] = field(default= """
    SELECT * FROM cypher('{schema_name}', $$
        MATCH (p1:{the_relavent_entity1} {{tuid:'{tuid1}'}}), 
              (p2:{the_relavent_entity2} {{tuid:'{tuid2}'}})
        CREATE (p1)-[r:{rel_type}]->(p2)
        RETURN r
    $$) AS (result agtype);
  """ )


class TestNodeGen:
  stack_entity=[]
  def create_node_id(self):
    r=str(uuid.uuid4())
    self.stack_entity.append(r)
    return r

  def __call__(self) -> Dict[str, Any]:
    """Generate random test data for insertion."""
    return {
      "type": "app_nodes", # is a class of query, EG: scraping something from some website app
      "title": "Title keyword",
      "summary": "A brief description",
      "time": datetime.now(tz=timezone.utc),
      "embedding": doc2vector_generator(),
      "data": {"key1": "value1", "key2": "value2"}, # json
      "tuid": self.create_node_id()
    }


class TestRelationGen:
  # type is AGE table
  get_inserted_tuid="""
  select type,tuid from {schema_name}.{ht_name};
  """
  def __init__(self) -> None:

    db_handler = DatabaseHandler(Const, RecordData , TripleData)
    db_handler.perform_vertex_insert(TestNodeGen()())
    db_handler.perform_vertex_insert(TestNodeGen()())
    app_q=db_handler._preprocess_query(self.get_inserted_tuid)
    stack_entity=db_handler._execute_fetch(app_q)
    type1 , tuid1 = stack_entity.pop(random.randint(0, len(stack_entity)-1))
    type2 , tuid2 = stack_entity.pop(random.randint(0, len(stack_entity)-1))
    self.head = (type1 , tuid1) 
    self.tail = (type2 , tuid2) 
    self.edge= db_handler.Const.TEST_EDGE_NAME_IS_TAB # no space permitted, is a table name

  def __call__(self) -> tuple:
    return (self.head,self.edge,self.tail)


class DatabaseTestManager:
  def __init__(self) -> None:
    self.db_handler = DatabaseHandler( Const=Const,  RecordValidator=RecordData , TripleValidator=TripleData  )

  def _perform_debug_insert_entity(self) -> None:
    self.db_handler.perform_vertex_insert(TestNodeGen()())

  def _perform_debug_insert_relation(self) ->None:
    self.db_handler.perform_edge_insert(TestRelationGen()())
    self.db_handler.close()


if __name__ == "__main__":
  db_manager = DatabaseTestManager()
  db_manager._perform_debug_insert_entity()
  db_manager._perform_debug_insert_relation()

