"""
mochow config
"""
from pydantic import BaseModel, SecretStr
from ..api import DBConfig, DBCaseConfig, MetricType, IndexType
from typing import Optional, List, Dict

class MochowConfig(DBConfig):
    """
    mochow config
    """
    host = "http://localhost:19530"
    account = "root"
    api_key = "mcqlwzjtxsrbsj"

    def to_dict(self) -> dict:
        """
        将对象转换为字典。
        
        Args:
            无参数。
        
        Returns:
            返回一个包含 host、account 和 api_key 的字典，分别对应对象的属性值。
        
        """
        return {
            "host": self.host,
            "account" : self.account,
            "api_key" : self.api_key,
        }


class MochowIndexConfig(BaseModel):
    """Base config for milvus"""

    index: IndexType
    metric_type = MetricType.L2

    def parse_metric(self) -> str:
        """
        解析指标类型
        
        Args:
            无参数
        
        Returns:
            返回指标类型的字符串表示，如果指标类型为空则返回空字符串
        
        """
        if not self.metric_type:
            return ""

        if self.metric_type == MetricType.COSINE:
            return MetricType.L2.value
        return self.metric_type.value


class HNSWConfig(MochowIndexConfig, DBCaseConfig):
    """hnsw config"""

    M: int
    efConstruction: int
    ef: int
    index: IndexType = IndexType.HNSW

    def index_param(self) -> dict:
        """
        返回包含索引参数的字典
        
        Returns:
            Dict[str, Any]: 包含索引类型和参数的字典
        """
        return {
            "metric_type": self.parse_metric(),
            "index_type": self.index.value,
            "params": {"M": self.M, "efConstruction": self.efConstruction},
        }

    def search_param(self) -> dict:
        """搜索参数字典
        
        Args:
            无
        
        Returns:
            dict: 返回包含 metric_type 和 params 的字典，其中 metric_type 是指标类型，params 中是具体的参数值。
        
        """
        return {
            "metric_type": self.parse_metric(),
            "params": {"ef": self.ef},
        }


_mochow_case_config = {
    IndexType.HNSW: HNSWConfig,
}
