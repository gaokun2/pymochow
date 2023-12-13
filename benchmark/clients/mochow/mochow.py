"""
mochow client
"""
import logging
import time
from contextlib import contextmanager
from typing import Iterable, Type, List
import json
from ..api import VectorDB, DBCaseConfig, DBConfig, IndexType
from .config import MochowConfig, _mochow_case_config
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.services.mochow.mochow_client import MochowClient
from baidubce.auth.bce_credentials import BceCredentials

log = logging.getLogger(__name__)

class Mochow(VectorDB):
    """mochow client"""

    def __init__(
        self,
        dim: int,
        db_config: DBConfig,
        db_case_config: DBCaseConfig,
        db_name: str = "benchmark_db",
        table_name: str = "benchmark_table",
        drop_old: bool = False,
        name: str = "Mochow",
        **kwargs,
    ):
        """Initialize wrapper around the milvus vector database."""
        self.name = name
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config
        self.db_name = db_name
        self.table_name = table_name

        self._primary_field = "id"
        self._vector_field = "vector"
        self._index_name = "vector_idx"
        
        account = self.db_config.account
        api_key = self.db_config.api_key
        host = self.db_config.host
        config = BceClientConfiguration(credentials=BceCredentials(account,
            api_key), endpoint=host)
        mochow_client = MochowClient(config)
        self.client = mochow_client
    
    def connect(self):
        """connect"""
        self.client.connect()

    def disconnect(self):
        """disconnect"""
        self.client.disconnect()

    @contextmanager
    def init(self) -> None:
        """
        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
            >>>     self.search_embedding()
        """
        response = self.client.list_databases()
        log.info("list databases response:%s", response)
        database_exist = False
        for database in response.databases:
            if database == self.db_name:
                database_exist = True
                break

        if not database_exist:
            response = self.client.create_database(self.db_name)
            log.info("create database %s response:%s", self.db_name, response)

        partition = {
            "partitionType": "HASH",
            "partitionNum": 1
        }
        fields = [
            {
                "fieldName": self._primary_field,
                "fieldType": "UINT64",
                "primaryKey": True,
                "partitionKey": True,
                "autoIncrement": True
            },
            {
                "fieldName": self._vector_field,
                "fieldType": "FLOAT_VECTOR",
                "dimension": self.dim
            }
        ]
        indexes = [
            {
                "field": self._vector_field,
                "indexName": self._index_name,
                "indexType": self.case_config.index.value,
                "metricType": self.case_config.metric_type.value,
                "params": {
                    "M": self.case_config.M,
                    "efConstruction": self.case_config.efConstruction
                },
                "autoBuild": False
            }
        ]
        fields_str = json.dumps(fields);
        indexes_str = json.dumps(indexes);
        response = self.client.list_tables(self.db_name)
        log.info("list tables db:%s, response:%s", self.db_name, response)
        table_exist = False
        for table in response.tables:
            if (table == self.table_name):
                table_exist = True
                break
        if not table_exist:
            response = self.client.create_table(self.db_name, self.table_name,
                    partition=partition, fields=fields, indexes=indexes,
                    replication=1)
            log.info("create table %s resposne:%s", self.table_name, response)

    def _post_insert(self):
        """
        根据给定的信息，重建向量索引。
        
        Args:
            无参数
        
        Returns:
            无返回值
        
        """
        log.info(f"{self.name} post insert before optimize")
        response = self.client.rebuild_vector_index(self.db_name, self.table_name,
                self._index_name)
        log.info("rebuild vector index: %s, response: %s", self._index_name, response)
        
        while True:
            response = self.client.desc_index(self.db_name, self.table_name, self._index_name)
            log.info("desc index response%s", response)
            time.sleep(10)
            if response.index['state'] == u"NORMAL":
                break

    def need_normalize_cosine(self) -> bool:
        """Wheather this database need to normalize dataset to support COSINE"""
        return False
    
    def insert_embeddings(
        self,
        embeddings: List[List[float]],
        metadata: List[int],
        **kwargs,
    ) -> (int, Exception):
        """Insert embeddings into Mochow. should call self.init() first"""
        # use the first insert_embeddings to init collection
        rows = [{"vector": embedding.tolist()} for embedding in embeddings]
        response = self.client.upsert_row(self.db_name, self.table_name,
                rows, keep_alive=True)
        insert_count = len(metadata)
        if kwargs.get("last_batch"):
            self._post_insert()
        return (insert_count, None)

    def search_embedding(
        self,
        query: List[float],
        k=10,
        filters=None,
    ) -> List[int]:
        """Perform a search on a query embedding and return results."""
        anns = {
            "vectorField": self._vector_field,
            "vectorFloats": query,
            "params": {
                "ef": self.case_config.ef,
                "limit": k
            }
        }
        response = self.client.search_row(self.db_name, self.table_name, anns,
                keep_alive=True)
        log.info("search response:%s", response)
        ids = [int(item['id'] - 1) for item in response.rows]
        return ids

