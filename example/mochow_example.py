"""
Examples for mochow client
"""

import mochow_example_conf
from baidubce.services.mochow.mochow_client import MochowClient

if __name__ == "__main__":
    import logging
    
    logging.basicConfig(filename='example.log', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    __logger = logging.getLogger(__name__)

    
    mochow_client = MochowClient(mochow_example_conf.config)
    ######################################################################################################
    #               database operation examples
    ######################################################################################################
    database_name = "test_db"
    database_exist = False
    response = mochow_client.list_databases()
    for database in response.databases:
        if (database == database_name):
            database_exist = True
        __logger.debug("list databases:%s", database)

    mochow_client.drop_database(database_name)

    __logger.debug("try to create database:%s", database_name)
    response = mochow_client.create_database(database_name)
    __logger.debug("create database response code:%s msg:%s", response.code,
            response.msg)
    


