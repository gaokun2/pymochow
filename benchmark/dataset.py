"""
Usage:
    >>> from xxx.dataset import Dataset
    >>> Dataset.Cohere.get(100_000)
"""

import os
import pathlib
import pandas as pd
from enum import Enum
from base import BaseModel
from config import Config
from clients import MetricType
import utils
import logging
import h5py
from typing import Optional, List

log = logging.getLogger(__name__)


class BaseDataset(BaseModel):
    name: str
    size: int
    dim: int
    metric_type: MetricType
    use_shuffled: bool
    _size_label: dict = {}
    
    @classmethod
    def verify_size(cls, v):
        """
        验证输入的大小是否在支持范围内
        
        Args:
            v (int | str): 需要验证的大小，可以是整数或字符串类型。
        
        Returns:
            int | str: 返回输入值，即输入值的校验结果
        
        Raises:
            ValueError: 当输入值不在预定义的支持范围时引发此异常。
        
        """
        if v not in cls._size_label:
            raise ValueError("Size {} not supported for the dataset, expected: {}".format(v, cls._size_label.keys()))
        return v

    @property
    def label(self):
        """
        获取对象大小的标签
        
        Returns:
            str: 对象大小的标签，如果没有找到则返回None。
        
        """
        return self._size_label.get(self.size)

    @property
    def dir_name(self):
        """
        获取当前类的目录名。
        
        Returns:
            返回格式为"{name}_{label}_{size}"的字符串，其中name为类名称、label为类标签和size为类大小。
        """
        return "{}_{}_{}".format(self.name, self.label, utils.numerize(self.size)).lower()


class Cohere(BaseDataset):
    name: str = "Cohere"
    dim: int = 768
    metric_type: MetricType = MetricType.COSINE
    use_shuffled: bool = Config.USE_SHUFFLED_DATA
    _size_label: dict = {
        100000: "SMALL",
        1000000: "MEDIUM",
        10000000: "LARGE",
    }

class DatasetManager(BaseModel):
    data:   BaseDataset
    train_data: Optional[pd.DataFrame] = None
    neighbors: Optional[pd.DataFrame] = None

    def __eq__(self, obj):
        """
        检查两个DatasetManager对象的名称和标签是否相同。
        
        Args:
            obj (DatasetManager): 要比较的另一个DatasetManager对象。
        
        Returns:
            bool: 如果两个DatasetManager对象的名称和标签相同，则返回True；否则，返回False。
        
        """
        if isinstance(obj, DatasetManager):
            return self.data.name == obj.data.name and self.data.label == obj.data.label
        return False

    @property
    def data_dir(self):
        """
        获取数据目录
        
        Returns:
            返回一个pathlib.Path对象，表示数据目录的路径。
        
        """
        return pathlib.Path(Config.DATASET_LOCAL_DIR, self.data.name.lower(), self.data.dir_name.lower())
    
    @property
    def test_data(self):
        """
             读取测试数据并返回pandas DataFrame对象
        
        Args:
            None
        
        Returns:
            pandas.DataFrame：一个包含测试数据的DataFrame对象
        
        """
        file_path = os.path.join(self.data_dir, "test.hdf5")
        if not os.path.exists(file_path):
            log.warning("No such file: {}".format(file_path))
            return pd.DataFrame()
        log.info("Read the entire file into memory: {}".format(file_path))
        file = h5py.File(file_path, 'r')
        data = file["test"][:]
        return pd.DataFrame(data)
    
    @property
    def neighbors(self):
        """
             获取邻居点的DataFrame数据
        
        Args:
            无参数
        
        Returns:
            pandas.DataFrame类型，包含邻居点信息
        """
        file_path = os.path.join(self.data_dir, "test.hdf5")
        if not os.path.exists(file_path):
            log.warning("No such file: {}".format(file_path))
            return pd.DataFrame()
        log.info("Read the entire file into memory: {}".format(file_path))
        file = h5py.File(file_path, 'r')
        data = file["neighbors"][:]
        return pd.DataFrame(data)

    def __iter__(self):
        """
        返回DataSetIterator对象
        
        Args:
            self: 实例对象，类型为DataSet
        
        Returns:
            DataSetIterator: 返回一个迭代器，用于遍历DataSet中的元素
        """
        return DataSetIterator(self)

    def _read_file(self, file_name):
        """
        读取指定的文件并返回其数据。
        
        Args:
            file_name (str): 文件名，包含路径。
        
        Returns:
            pandas.DataFrame: 返回文件中内容的 DataFrame 对象。
        
        """
        file_path = os.path.join(self.data_dir, file_name)
        if not os.path.exists(file_path):
            log.warning("No such file: {}".format(file_path))
            return pd.DataFrame()
        log.info("Read the entire file into memory: {}".format(file_path))
        file = h5py.File(file_path, 'r')
        data = file["train"][:]
        self.train_data = pd.DataFrame(data)
        data = file["test"][:]
        self.test_data = pd.DataFrame(data)
        data = file["neighbors"][:]
        self.neighbors = pd.DataFrame(data)
        file.close()
    
    def _validate_local_file(self):
        """
        验证本地文件的有效性。
        
        Args:
            无参数，默认值为空字典。
        
        Returns:
            无返回值，直接操作文件系统。
        
        Raises:
            None。
        """
        if not self.data_dir.exists():
            log.info(f"local file path not exist, creating it: {self.data_dir}")
            self.data_dir.mkdir(parents=True)

    def prepare(self, check=True) -> bool:
        """准备读取文件。
        
        Args:
            check (bool): 用于验证是否需要检查本地文件。默认为True。
        
        Returns:
            bool: 如果成功，则返回True；否则返回False。
        
        """
        if check:
            self._validate_local_file()

        self._read_file("test.hdf5")
        return True

class DataSetIterator:
    def __init__(self, dataset: DatasetManager):
        """初始化DatasetManager类。
        
        Args:
            dataset (DatasetManager): 数据集管理器对象，用于管理数据集。
        
        """
        self._ds = dataset
        file_path = os.path.join(dataset.data_dir, "test.hdf5")
        self.file = h5py.File(file_path, 'r')
        self.dataset = self.file["train"]
        self.total_size = self.dataset.shape[0]
        self.current_index = 0
        self.batch_size = Config.NUM_PER_BATCH
    
    def __next__(self):
        """
        实现__next__()方法，用于迭代数据集。
        
        Args:
            无。
        
        Returns:
            数据集中指定范围内的子集。
        
        Raises:
            StopIteration: 当迭代结束时，会引发该异常。
        """
        if self.current_index >= self.total_size:
            raise StopIteration
        start, self.current_index = self.current_index, self.current_index + self.batch_size
        end = min(self.current_index, self.total_size)
        return self.dataset[start:end]

    def close(self):
        """
        关闭文件。
        
        Args:
            无参数。
        
        Returns:
            无返回值。
        
        """
        self.file.close()

class Dataset(Enum):
    COHERE = Cohere

    def get(self, size):
        """
        获取Cohere对象实例
        
        Args:
            size (int): 对象的大小，单位为位（bit）
        
        Returns:
            Cohere: 返回一个Cohere对象实例
        
        """
        return Cohere(size=size)

    def manager(self, size):
        """
        从数据集中获取指定数量的数据，并返回一个DatasetManager对象。
        
        Args:
            size (int): 要获取的数据量。
        
        Returns:
            DatasetManager: 返回一个DatasetManager对象，包含从数据集中获取的数据。
        
        """
        return DatasetManager(data=self.get(size))
