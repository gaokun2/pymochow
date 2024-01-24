"""
This module provide enum.
"""
from enum import Enum, unique


@unique
class MetricType(Enum):
    """
    The metric type of the vector index.
    """
    L2 = "L2"


@unique
class IndexType(Enum):
    """index type"""
    # vector index type
    HNSW = "HNSW"
    FLAT = "FLAT"
    
    # scalar index type
    SECONDARY_INDEX = "SECONDARY"


@unique
class FieldType(Enum):
    """
    Field type
    """
    # scalar field type
    BOOL = "BOOL"
    INT8 = "INT8"
    UINT8 = "UINT8"
    INT16 = "INT16"
    UINT16 = "UINT16"
    INT32 = "INT32"
    UINT32 = "UINT32"
    INT64 = "INT64"
    UINT64 = "UINT64"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    STRING = "STRING"
    BINARY = "BINARY"
    UUID = "UUID"

    # vector field type
    FLOAT_VECTOR = "FLOAT_VECTOR"


@unique
class PartitionType(Enum):
    """
    Partition Type
    """
    HASH = "HASH"


@unique
class ServerErrCode(Enum):
    """
    Server error no
    """
    INTERNAL_ERROR = 1
    INVALID_PARAMETER = 2

    INVALID_HTTP_URL = 10
    INVALID_HTTP_HEADER = 11
    INVALID_HTTP_BODY = 12
    MISS_SSL_CERTIFICATES = 13

    USER_NOT_EXIST = 20
    USER_ALREADY_EXIST = 21
    ROLE_NOT_EXIST = 22
    ROLE_ALREADY_EXIST = 23
    AUTHENTICATION_FAILED = 24
    PERMISSION_DENIED = 25

    # Database errors
    DB_NOT_EXIST = 50
    DB_ALREADY_EXIST = 51
    DB_TOO_MANY_TABLES = 52
    DB_NOT_EMPTY = 53

    # Table errors
    INVALID_TABLE_SCHEMA = 60
    INVALID_PARTITION_PARAMETERS = 61
    TABLE_TOO_MANY_FIELDS = 62
    TABLE_TOO_MANY_FAMILIES = 63
    TABLE_TOO_MANY_PRIMARY_KEYS = 64
    TABLE_TOO_MANY_PARTITION_KEYS = 65
    TABLE_TOO_MANY_VECTOR_FIELDS = 66
    TABLE_TOO_MANY_INDEXES = 67
    DYNAMIC_SCHEMA_ERROR = 68
    TABLE_NOT_EXIST = 69
    TABLE_ALREADY_EXIST = 70
    INVALID_TABLE_STATE = 71
    TABLE_NOT_READY = 72
    ALIAS_NOT_EXIST = 73
    ALIAS_ALREADY_EXIST = 74

    # Field errors
    FIELD_NOT_EXIST = 80
    FIELD_ALREADY_EXIST = 81
    VECTOR_FIELD_NOT_EXIST = 82

    # Index errors
    INVALID_INDEX_SCHEMA = 90
    INDEX_NOT_EXIST = 91
    INDEX_ALREADY_EXIST = 92
    INDEX_DUPLICATED = 93
    INVALID_INDEX_STATE = 94


@unique
class TableState(Enum):
    """
    Table State
    """
    CREATING = "CREATING"
    NORMAL = "NORMAL"
    DELETING = "DELETING"


@unique
class ReadConsistency(Enum):
    """
    Read Consistency
    """
    EVENTUAL = "EVENTUAL"
    STRONG = "STRONG"


@unique
class IndexState(Enum):
    """
    Index State
    """
    BUILDING = "BUILDING"
    NORMAL = "NORMAL"
