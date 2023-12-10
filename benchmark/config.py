"""
config
"""
import environs
import inspect
import pathlib

env = environs.Env()
env.read_env(".env")

class Config:
    """Config"""

    DATASET_LOCAL_DIR = env.path("DATASET_LOCAL_DIR", "./dataset")
    NUM_PER_BATCH = env.int("NUM_PER_BATCH", 1000)

    DROP_OLD = env.bool("DROP_OLD", True)
    USE_SHUFFLED_DATA = env.bool("USE_SHUFFLED_DATA", True)

    RESULTS_LOCAL_DIR = pathlib.Path(__file__).parent.joinpath("results")

    CAPACITY_TIMEOUT_IN_SECONDS = 24 * 3600 # 24h
    LOAD_TIMEOUT_DEFAULT        = 2.5 * 3600 # 2.5h
    LOAD_TIMEOUT_768D_1M        = 2.5 * 3600 # 2.5h
    LOAD_TIMEOUT_768D_10M       =  25 * 3600 # 25h
    LOAD_TIMEOUT_768D_100M      = 250 * 3600 # 10.41d

    LOAD_TIMEOUT_1536D_500K     = 2.5 * 3600 # 2.5h
    LOAD_TIMEOUT_1536D_5M       =  25 * 3600 # 25h

    OPTIMIZE_TIMEOUT_DEFAULT    = 15 * 60   # 15min
    OPTIMIZE_TIMEOUT_768D_1M    =  15 * 60   # 15min
    OPTIMIZE_TIMEOUT_768D_10M   = 2.5 * 3600 # 2.5h
    OPTIMIZE_TIMEOUT_768D_100M  =  25 * 3600 # 1.04d


    OPTIMIZE_TIMEOUT_1536D_500K =  15 * 60   # 15min
    OPTIMIZE_TIMEOUT_1536D_5M   =   2.5 * 3600 # 2.5h
    def display(self) -> str:
        """
        显示对象属性的名称列表。
        
        Args:
            无参数。
        
        Returns:
            返回一个字符串，包含对象的所有属性名，不包括方法和私有属性（以前缀_开头）。
        
        """
        tmp = [
            i for i in inspect.getmembers(self)
            if not inspect.ismethod(i[1])
            and not i[0].startswith('_')
            and "TIMEOUT" not in i[0]
        ]
        return tmp
