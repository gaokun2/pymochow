"""
metric
"""
import logging
import numpy as np
from typing import Optional, List

from dataclasses import dataclass


log = logging.getLogger(__name__)


@dataclass
class Metric:
    """result metrics"""

    # for load cases
    max_load_count: int = 0

    # for performance cases
    load_duration: float = 0.0  # duration to load all dataset into DB
    qps: float = 0.0
    serial_latency_p99: float = 0.0
    recall: float = 0.0


QURIES_PER_DOLLAR_METRIC = "QP$ (Quries per Dollar)"
LOAD_DURATION_METRIC = "load_duration"
SERIAL_LATENCY_P99_METRIC = "serial_latency_p99"
MAX_LOAD_COUNT_METRIC = "max_load_count"
QPS_METRIC = "qps"
RECALL_METRIC = "recall"

metricUnitMap = {
    LOAD_DURATION_METRIC: "s",
    SERIAL_LATENCY_P99_METRIC: "ms",
    MAX_LOAD_COUNT_METRIC: "K",
    QURIES_PER_DOLLAR_METRIC: "K",
}

lowerIsBetterMetricList = [
    LOAD_DURATION_METRIC,
    SERIAL_LATENCY_P99_METRIC,
]

metricOrder = [
    QPS_METRIC,
    RECALL_METRIC,
    LOAD_DURATION_METRIC,
    SERIAL_LATENCY_P99_METRIC,
    MAX_LOAD_COUNT_METRIC,
]


def isLowerIsBetterMetric(metric: str) -> bool:
    """
    判断给定的指标是否表示较小更好。
    
    Args:
        metric (str): 要判断的指标。
    
    Returns:
        bool: 如果指标表示较小更好，则返回True；否则返回False。
    """
    return metric in lowerIsBetterMetricList


def calc_recall(count: int, ground_truth: List[int], got: List[int]) -> float:
    """计算召回率
    
    Args:
        count (int): 样本数量
        ground_truth (List[int]): 真实标签列表
        got (List[int]): 预测标签列表
    
    Returns:
        float: 召回率，为平均值
    
    """
    recalls = np.zeros(count)
    for i, result in enumerate(got):
        if result in ground_truth:
            recalls[i] = 1

    return np.mean(recalls)
