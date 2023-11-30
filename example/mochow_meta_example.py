"""
Examples for mochow client
"""

import logging
import time
import json
import random
import string
import mochow_example_conf
from baidubce.services.mochow.mochow_client import MochowClient

logging.basicConfig(filename='example.log', level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__logger = logging.getLogger(__name__)


def generate_random_string(length):
    """
    generate_random_string
    """
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for i in range(length))
    return random_string


def create_database(database_name):
    """
    create_database
    """
    mochow_client = MochowClient(mochow_example_conf.config)
    response = mochow_client.create_database(database_name)
    if response.code != 0:
        __logger.error("fail to create database:%s", response)
    __logger.debug("create database response:%s", response)


def drop_database(database_name):
    """
    drop database
    """
    mochow_client = MochowClient(mochow_example_conf.config)
    response = mochow_client.list_databases()
    __logger.debug("list database response:%s", response)
    for database in response.databases:
        list_tables_response = mochow_client.list_tables(database)
        for table in list_tables_response.tables:
            response = mochow_client.drop_table(database, table)
            if response.code != 0:
                __logger.error("fail to drop table:%s", response)
        response = mochow_client.drop_database(database_name)
        if response.code != 0:
            __logger.error("fail to drop database:%s", response)


def create_table(database_name, table_name):
    """
    create database
    """
    mochow_client = MochowClient(mochow_example_conf.config)
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
    response = mochow_client.create_table(database_name, table_name,
            partition=partition, fields=fields, indexes=indexes)
    if response.code != 0:
        __logger.error("fail to create table:%s", response)


def add_field(database_name, table_name):
    """
    add field
    """
    mochow_client = MochowClient(mochow_example_conf.config)
    field_types = ["BOOL", "INT8", "UINT8", "INT16", "UINT16", "INT32",
            "UINT32", "INT64", "UINT64", "FLOAT", "DOUBLE", "DATE", "DATETIME",
            "TIME", "TIMESTAMP", "HLC", "STRING", "BINARY", "FLOAT_VECTOR"]
    fields = [{
        "fieldName": generate_random_string(10),
        "fieldType": random.choice(field_types)
    }]
    response = mochow_client.add_table_field(database_name, table_name,
            fields=fields)
    if response.code != 0:
        __logger.error("fail to add field:%s", response)


if __name__ == "__main__":
    database_name = "test"
    drop_database(database_name)

    table_name = "book-vector"
    create_database(database_name)
    create_table(database_name, table_name)
    add_field(database_name, table_name)
