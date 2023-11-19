"""
Examples for mochow client
"""

import json
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
    database_name = "db-test"
    database_exist = False
    response = mochow_client.list_databases()
    for database in response.databases:
        if (database == database_name):
            database_exist = True
        __logger.debug("list databases:%s", database)

    if database_exist:
        mochow_client.drop_database(database_name)

    __logger.debug("try to create database:%s", database_name)
    response = mochow_client.create_database(database_name)
    __logger.debug("create database response code:%s msg:%s", response.code,
            response.msg)
    
    ######################################################################################################
    #               table operation examples
    ######################################################################################################
    table_name = "book-vector"
   
    table_exist = False
    response = mochow_client.list_tables(database_name)
    for table in response.tables:
        if (table == table_name):
            table_exist = True
        __logger.debug("list tables:%s", database)
    
    if table_exist:
        mochow_client.drop_table(database_name, table_name)

    partition = {
        "partitionType": "HASH",
        "partitionNum": 10
    }
    fields = [
        {
            "fieldName": "id",
            "fieldType": "UINT64",
            "primaryKey": True,
            "partitionKey": True,
            "autoIncrement": True
        },
        {
            "fieldName": "bookName",
            "fieldType": "STRING"
        },
        {
            "fieldName": "author",
            "fieldType": "STRING"
        },
        {
            "fieldName": "vector",
            "fieldType": "FLOAT_VECTOR",
            "dimension": 768
        }
    ]
    indexes = [
        {
            "field": "vector",
            "indexName": "vector_idx",
            "indexType": "HNSW",
            "metricType": "L2",
            "params": {
                "M": 32,
                "efConstruction": 200
            },
            "autoBuild": False
        }
    ]
    fields_str = json.dumps(fields);
    indexes_str = json.dumps(indexes);
    __logger.debug("try to create table:%s", table_name)
    response = mochow_client.create_table(database_name, table_name,
            partition=partition, fields=fields, indexes=indexes)
    
    response = mochow_client.desc_table(database_name, table_name)
    __logger.debug("desc table:%s %s", table_name, response)

