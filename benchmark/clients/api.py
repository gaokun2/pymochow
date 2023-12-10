"""
vector client api
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Type
from typing import Optional, List, Dict
from contextlib import contextmanager

from pydantic import BaseModel, validator, SecretStr


class MetricType(str, Enum):
    """
    Metric type
    """
    L2 = "L2"
    COSINE = "COSINE"
    IP = "IP"


class IndexType(str, Enum):
    """
    Index type
    """
    HNSW = "HNSW"
    DISKANN = "DISKANN"
    IVFFlat = "IVF_FLAT"
    Flat = "FLAT"
    AUTOINDEX = "AUTOINDEX"
    ES_HNSW = "hnsw"


class DBConfig(ABC, BaseModel):
    """DBConfig contains the connection info of vector database

    Args:
        db_label(str): label to distinguish different types of DB of the same database.

            MilvusConfig.db_label = 2c8g
            MilvusConfig.db_label = 16c64g
            ZillizCloudConfig.db_label = 1cu-perf
    """

    db_label: str = ""

    @abstractmethod
    def to_dict(self) -> dict:
        """
        
        抛出NotImplementedError异常，表示未实现方法
        
        Args:
            无参数
        
        Returns:
            dict: 返回一个空字典（{}）
        
        Raises:
            NotImplementedError: 当类未实现to_dict()方法时，抛出此异常。
        
        """
        raise NotImplementedError

    @validator("*")
    def not_empty_field(cls, v, field):
        """Check whether the value of a given field is empty.
        
        Args:
            cls (Any): The class object that contains this function.
        
            v (Any): The value to be checked.
        
            field (Field): A Field instance representing the specific field
                being validated.
        
        Returns:
            Any: The same value as input `v`.
        
        Raises:
            ValueError: If the field name equals 'db_label' or the value
                is an empty string, raise a ValueError exception with a 
                message indicating that it should not be empty.
        
        """
        if field.name == "db_label":
            return v
        if isinstance(v, (str, SecretStr)) and len(v) == 0:
            raise ValueError("Empty string!")
        return v


class DBCaseConfig(ABC):
    """Case specific vector database configs, usually uesed for index params like HNSW"""
    @abstractmethod
    def index_param(self) -> dict:
        """
        
        Args:
            无参数。
        
        Returns:
            dict类型，表示索引参数。
        
        Raises:
            NotImplementedError：当该方法没有被实现时，会触发该异常。
        
        """
        raise NotImplementedError

    @abstractmethod
    def search_param(self) -> dict:
        """
        
        Args:
            无参数。
        
        Returns:
            dict: 未实现的抽象方法，抛出NotImplementedError。
        
        Raises:
            NotImplementedError: 当子类未实现此方法时，会抛出该异常。
        """
        raise NotImplementedError


class EmptyDBCaseConfig(BaseModel, DBCaseConfig):
    """EmptyDBCaseConfig will be used if the vector database has no case specific configs"""
    null : Optional[str] = None
    def index_param(self) -> dict:
        """
        未定义函数
        
        Args:
            无参
        
        Returns:
            一个空字典，表示索引参数为空
        
        """
        return {}

    def search_param(self) -> dict:
        """
        获取搜索参数的方法
        
        Args:
            无参数
        
        Returns:
            返回一个空字典，即{}
        
        """
        return {}


class VectorDB(ABC):
    """Each VectorDB will be __init__ once for one case, the object will be copied into multiple processes.

    In each process, the benchmark cases ensure VectorDB.init() calls before any other methods operations

    insert_embeddings, search_embedding, and, optimize will be timed for each call.

    Examples:
        >>> milvus = Milvus()
        >>> with milvus.init():
        >>>     milvus.insert_embeddings()
        >>>     milvus.search_embedding()
    """

    @abstractmethod
    def __init__(
        self,
        dim: int,
        db_config: DBConfig,
        db_case_config: Optional[DBCaseConfig],
        databse_name: str,
        table_name: str,
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        """Initialize wrapper around the vector database client.

        Please drop the existing collection if drop_old is True. And create collection
        if collection not in the Vector Database

        Args:
            dim(int): the dimension of the dataset
            db_config(dict): configs to establish connections with the vector database
            db_case_config(DBCaseConfig | None): case specific configs for indexing and searching
            drop_old(bool): whether to drop the existing collection of the dataset.
        """
        raise NotImplementedError

    @abstractmethod
    @contextmanager
    def init(self) -> None:
        """ create and destory connections to database.

        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
        """
        raise NotImplementedError

    def need_normalize_cosine(self) -> bool:
        """Wheather this database need to normalize dataset to support COSINE"""
        return False

    @abstractmethod
    def insert_embeddings(
        self,
        embeddings: List[List[float]],
        metadata: List[int],
        **kwargs,
    ) -> (int, Exception):
        """Insert the embeddings to the vector database. The default number of embeddings for
        each insert_embeddings is 5000.

        Args:
            embeddings(list[list[float]]): list of embedding to add to the vector database.
            metadatas(list[int]): metadata associated with the embeddings, for filtering.
            **kwargs(Any): vector database specific parameters.

        Returns:
            int: inserted data count
        """
        raise NotImplementedError

    @abstractmethod
    def search_embedding(
        self,
        query: List[float],
        k: int = 100,
        filters=None,
    ) -> List[int]:
        """Get k most similar embeddings to query vector.

        Args:
            query(list[float]): query embedding to look up documents similar to.
            k(int): Number of most similar embeddings to return. Defaults to 100.
            filters(dict, optional): filtering expression to filter the data while searching.

        Returns:
            list[int]: list of k most similar embeddings IDs to the query embedding.
        """
        raise NotImplementedError
