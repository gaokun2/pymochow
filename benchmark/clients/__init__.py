"""
vector clients
"""
from enum import Enum
from typing import Type, Optional
from .api import (
    VectorDB,
    DBConfig,
    DBCaseConfig,
    EmptyDBCaseConfig,
    IndexType,
    MetricType,
)


class DB(Enum):
    """Database types

    Examples:
        >>> DB.Mochow
        <DB.Mochow: 'Mochow'>
        >>> DB.Mochow.value
        "Mochow"
        >>> DB.Mochow.name
        "Mochow"
    """

    Mochow = "Mochow"


    @property
    def init_cls(self) -> Type[VectorDB]:
        """Import while in use"""
        if self == DB.Mochow:
            from .mochow.mochow import Mochow
            return Mochow


    @property
    def config_cls(self) -> Type[DBConfig]:
        """Import while in use"""
        if self == DB.Mochow:
            from .mochow.config import MochowConfig
            return MochowConfig

    def case_config_cls(self, index_type: Optional[IndexType] = None) -> Type[DBCaseConfig]:
        """
        获取数据库类型的配置类
        
        Args:
            index_type (Optional[IndexType], optional): 索引类型，默认为空值。
        
        Returns:
            Type[DBCaseConfig]: 返回指定数据库类型的配置类实例。
        """
        if self == DB.Mochow:
            from .mochow.config import _mochow_case_config
            return _mochow_case_config.get(index_type)

        return EmptyDBCaseConfig


__all__ = [
    "DB", "VectorDB", "DBConfig", "DBCaseConfig", "IndexType", "MetricType", "EmptyDBCaseConfig",
]
