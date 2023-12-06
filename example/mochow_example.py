"""
Examples for mochow client
"""

import time
import json
import random
import mochow_example_conf
from baidubce.services.mochow.mochow_client import MochowClient

book_names = ["The Great Gatsby", "To Kill a Mockingbird", "1984", "Pride and Prejudice", "The Catcher in the Rye"]
authors = ["F. Scott Fitzgerald", "Harper Lee", "George Orwell", "Jane Austen", "J.D. Salinger"]

def generate_random_vector(dimension):
    """
    generate_random_vector
    """
    if dimension <= 0:
        raise ValueError("dimension error")

    random_vector = [random.uniform(0, 1) for _ in range(dimension)]
    return random_vector

if __name__ == "__main__":
    import logging
    
    logging.basicConfig(filename='example.log', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    __logger = logging.getLogger(__name__)

    
    mochow_client = MochowClient(mochow_example_conf.config)
    ######################################################################################################
    #               database operation examples
    ######################################################################################################
    database_name = "db_test"
    response = mochow_client.list_databases()
    for database in response.databases:
        list_tables_response = mochow_client.list_tables(database)
        for table in list_tables_response.tables:
            mochow_client.drop_table(database, table)
        mochow_client.drop_database(database)

    __logger.debug("try to create database:%s", database_name)
    response = mochow_client.create_database(database_name)
    __logger.debug("create database response code:%s msg:%s", response.code,
            response.msg)
    
    ######################################################################################################
    #               table operation examples
    ######################################################################################################
    table_name = "book_vector"
   
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
    
    ######################################################################################################
    #               row operation examples
    ######################################################################################################
    time.sleep(50)
    ## insert row
    for i in range(1000):
        __logger.info("insert row %s", i)
        book_name = random.choice(book_names)
        author = random.choice(authors)
        random_vector = generate_random_vector(768)
        rows = [
            {
                "bookName": book_name,
                "author": author,
                "vector": random_vector
            }
        ]
        try:
            response = mochow_client.insert_row(database_name, table_name, rows)
        except Exception as e:
            __logger.info("exception:%s", e)
        __logger.info("response:%s", response)

    # query row
    for i in range(100):
        j = random.randint(0, 9)
        __logger.info("query row %s for tp %s", i, j)
        row_id = (j << 40) + i
        primary_key = {
            "id": row_id
        }
        try:
            response = mochow_client.query_row(database_name, table_name,
                    primary_key)
        except Exception as e:
            __logger.info("exception:%s", e)
        __logger.info("response:%s", response)

    ## update row
    for i in range(100):
        j = random.randint(0, 9)
        __logger.info("update row %s for tp %s", i, j)
        row_id = (j << 40) + i
        primary_key = {
            "id": row_id
        }
        partition_key = {
            "id": row_id
        }
        update = {
            "bookName": random.choice(book_names),
            "author": random.choice(authors),
        }
        try:
            response = mochow_client.update_row(database_name, table_name,
                    primary_key, partition_key, update)
        except Exception as e:
            __logger.info("exception:%s", e)
        __logger.info("response:%s", response)
    
    ## delete row
    for i in range(100):
        j = random.randint(0, 9)
        __logger.info("delete row %s for tp %s", i, j)
        row_id = (j << 40) + i
        primary_key = {
            "id": row_id
        }
        partition_key = {
            "id": row_id
        }
        try:
            response = mochow_client.delete_row(database_name, table_name, primary_key, partition_key)
        except Exception as e:
            __logger.info("exception:%s", e)
        __logger.info("response:%s", response)

    ######################################################################################################
    #               rebuild vector index
    ######################################################################################################
    response = mochow_client.rebuild_vector_index(database_name, table_name, "vector_idx")
    __logger.info("rebuild vector index: %s", response)

    while 1:
        response = mochow_client.desc_index(database_name, table_name, "vector_idx")
        __logger.info("desc index: %s", response)
        time.sleep(10)
        if response.index.state == u"NORMAL":
            break
    ######################################################################################################
    #               search row
    ######################################################################################################
    anns = {
        "vectorField": "vector",
        "vectorFloats": generate_random_vector(768),
        "params": {
            "ef": 100,
            "limit": 10
        }
    }
    response = mochow_client.search_row(database_name, table_name, anns)
    __logger.info("search result: %s", response)
