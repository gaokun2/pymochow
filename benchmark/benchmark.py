"""
benchmark main
"""
import string
import random
from dataset import Dataset
from clients.mochow.config import MochowConfig, HNSWConfig
from clients.mochow.mochow import Mochow
from runner import SerialInsertRunner, SerialSearchRunner
from runner import MultiProcessingSearchRunner

def generate_random_string(length):
    """
    generate_random_string
    """
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for i in range(length))
    return random_string

if __name__ == "__main__":
    import logging
    logging.basicConfig(filename='example.log', level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    __logger = logging.getLogger(__name__)

    dataset_manager = Dataset.COHERE.manager(size=1_000_000)
    
    db_config = MochowConfig()
    db_config.host = "http://127.0.0.1:8511"
    hnsw_config = HNSWConfig(M=32, efConstruction=200, ef=200)
    
    db_name = "benchmark_db_" + generate_random_string(5)
    table_name = "benchmark_table_" + generate_random_string(5)
    #db_name = "benchmark_db_Zwvmk"
    #table_name = "benchmark_table_oMvpi"
    mochow = Mochow(dim=768, db_config=db_config, db_case_config=hnsw_config,
            db_name=db_name, table_name=table_name)
    
    runner = SerialInsertRunner(db=mochow, dataset=dataset_manager, normalize=True)
    runner.run()
    
    runner = SerialSearchRunner(db=mochow,
            test_data=dataset_manager.test_data.values.tolist(),
            ground_truth=dataset_manager.neighbors,
            k=10)
    runner.run()
    
    runner = MultiProcessingSearchRunner(db=mochow, 
            test_data=dataset_manager.test_data.values.tolist())
    runner.run()

