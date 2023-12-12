"""
serial runner
"""
import time
import logging
import traceback
import concurrent.futures
import multiprocessing as mp
import math
import psutil
import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Tuple

import utils
from clients import api
from metric import calc_recall
from config import Config
from dataset import DatasetManager

NUM_PER_BATCH = Config.NUM_PER_BATCH
LOAD_MAX_TRY_COUNT = 10
WAITTING_TIME = 60

log = logging.getLogger(__name__)

class SerialInsertRunner:
    """serial insert runner"""

    def __init__(self, db: api.VectorDB, dataset: DatasetManager, 
            normalize: bool, timeout: Optional[float]=None):
        """
        初始化方法
        
        Args:
            db (api.VectorDB): 数据库实例
            dataset (DatasetManager): 数据集管理器
            normalize (bool): 是否归一化
            timeout (Optional[float], optional): 超时时间，默认值为None. Defaults to None.
        
        Returns:
            None
        
        """
        self.timeout = timeout if isinstance(timeout, (int, float)) else None
        self.dataset = dataset
        self.db = db
        self.normalize = normalize

    def task(self) -> int:
        """
        任务函数，用于插入嵌入到VectorDB数据库中。
        
        Args:
            无参数
        
        Returns:
            int类型的返回值为插入的总数量
        
        """
        count = 0
        batch_size = Config.NUM_PER_BATCH
        log.info(f"({mp.current_process().name:16}) Start inserting embeddings in batch {Config.NUM_PER_BATCH}")
        self.db.init()
        time.sleep(50)
        start = time.perf_counter()
        for data_df in self.dataset:
            all_metadata = list(range(count, count + batch_size))
            emb_np = np.stack(data_df)
            if self.normalize:
                log.info("normalize the 100k train data")
                all_embeddings = emb_np / np.linalg.norm(emb_np, axis=1)[:, np.newaxis].tolist()
            else:
                all_embeddings = emb_np
            del(emb_np)
            log.info(f"batch dataset size: {len(all_embeddings)}, {len(all_metadata)}")
            last_batch = self.dataset.data.size - count == len(all_metadata)
            insert_count, error = self.db.insert_embeddings(
                embeddings=all_embeddings,
                metadata=all_metadata,
                last_batch=last_batch,
            )
            if error is not None:
                raise error
            insert_count = len(all_metadata)
            assert insert_count == len(all_metadata)
            count += insert_count
            if count % 100_000 == 0:
                log.info(f"({mp.current_process().name:16}) Loaded {count} embeddings into VectorDB")
        log.info(
            f"({mp.current_process().name:16}) Finish loading all dataset into VectorDB, "
            f"dur={time.perf_counter()-start}")
        return count
    
    @utils.time_it
    def _insert_all_batches(self) -> int:
        """Performance case only"""
        with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.task)
            try:
                count = future.result(timeout=self.timeout)
            except TimeoutError as e:
                msg = f"VectorDB load dataset timeout in {self.timeout}"
                log.warning(msg)
                for pid, _ in executor._processes.items():
                    psutil.Process(pid).kill()
                raise PerformanceTimeoutError(msg) from e
            except Exception as e:
                log.warning(f"VectorDB load dataset error: {e}")
                raise e from e
            else:
                return count

    def run(self) -> int:
        """
        执行插入操作
        
        Args:
            无参数，返回值为int类型，表示成功插入的行数
        
        Returns:
            返回值为成功插入的行数
        
        """
        count, dur = self._insert_all_batches()
        return count


class SerialSearchRunner:
    """serial search runner"""

    def __init__(
        self,
        db: api.VectorDB,
        test_data: List[List[float]],
        ground_truth: pd.DataFrame,
        k: int = 100,
        filters: Optional[Dict] = None,
    ):
        """初始化查询任务。
        
        Args:
            db (api.VectorDB): 向量数据库实例。
            test_data (List[List[float]]): 测试数据的列表，每个测试数据是一个特征向量。
            ground_truth (pd.DataFrame): 真实标签的pandas数据框。
            k (int): 召回率的阈值，默认为100。
            filters (Optional[Dict]): 可选的参数，过滤器的字典。
        
        Returns:
            None
        
        """
        self.db = db
        self.k = k
        self.filters = filters

        if isinstance(test_data[0], np.ndarray):
            self.test_data = [query.tolist() for query in test_data]
        else:
            self.test_data = test_data
        self.ground_truth = ground_truth

    def search(self, args: Tuple[List, pd.DataFrame]):
        """
        搜索整个测试数据以获取recall和延迟
        
        Args:
            args: Tuple[List, pd.DataFrame]: 包含两个元素的元组，第一个为测试数据列表，第二个为真实标签DataFrame
        
        Returns:
            Tuple[float, float]: 返回一个元组，第一个为平均recall值，第二个为平均延迟
        
        """
        log.info(f"{mp.current_process().name:14} start search the entire test_data to get recall and latency")
        self.db.init()
        self.db.connect()
        test_data, ground_truth = args

        log.debug(f"test dataset size: {len(test_data)}")
        log.debug(f"ground truth size: {ground_truth.columns}, shape: {ground_truth.shape}")

        latencies, recalls = [], []
        for idx, emb in enumerate(test_data):
            s = time.perf_counter()
            try:
                results = self.db.search_embedding(
                    emb,
                    self.k,
                    self.filters,
                )

            except Exception as e:
                log.warning(f"VectorDB search_embedding error: {e}")
                traceback.print_exc(chain=True)
                raise e from None

            latencies.append(time.perf_counter() - s)

            gt = ground_truth[idx]
            recalls.append(calc_recall(self.k, gt[:self.k], results))


            if len(latencies) % 100 == 0:
                log.info(
                    f"({mp.current_process().name:14}) search_count={len(latencies):3}, "
                    f"latest_latency={latencies[-1]}, latest recall={recalls[-1]}")

        self.db.disconnect()
        avg_latency = round(np.mean(latencies), 4)
        avg_recall = round(np.mean(recalls), 4)
        cost = round(np.sum(latencies), 4)
        p99 = round(np.percentile(latencies, 99), 4)
        log.info(
            f"{mp.current_process().name:14} search entire test_data: "
            f"cost={cost}s, "
            f"queries={len(latencies)}, "
            f"avg_recall={avg_recall}, "
            f"avg_latency={avg_latency}, "
            f"p99={p99}"
         )
        return (avg_recall, p99)


    def _run_in_subprocess(self) -> Tuple[float, float]:
        """
        运行在子进程中
        
        Args:
            无参数，默认值None
        
        Returns:
            包含两个元素的元组，分别为浮点型数据：
                - 测试数据集准确率
                - 真实数据集准确率
        
        Raises:
            无异常处理机制
        
        """
        #mp.set_start_method('spawn', force=True)
        with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.search, (self.test_data, self.ground_truth))
            result = future.result()
            return result

    def run(self) -> Tuple[float, float]:
        """
        执行子进程并返回运行结果
        
        Args:
            无参数
        
        Returns:
            返回一个元组，包含两个元素：
                - 第一个元素为float类型，表示进程的运行时长（单位：秒）
                - 第二个元素为float类型，表示进程的退出码
        
        Raises:
            无异常抛出
        
        """
        return self._run_in_subprocess()
