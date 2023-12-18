"""
Examples for mochow client
"""

import time
import json
import random
import string
import mochow_example_conf
from baidubce.services.mochow.mochow_client import MochowClient

book_names = ["The Great Gatsby", "To Kill a Mockingbird", "1984", "Pride and Prejudice", "The Catcher in the Rye"]
authors = ["F. Scott Fitzgerald", "Harper Lee", "George Orwell", "Jane Austen", "J.D. Salinger"]

def generate_random_string(length):
    """
    generate_random_string
    """
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for i in range(length))
    return random_string

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
    
    while True:
        database_name = "db_test_" + generate_random_string(5);
        response = mochow_client.list_databases()
        __logger.info("list databases response:%s", response)
        database_exist = False
        for database in response.databases:
            if database == database_name:
                database_exist = True
                break
        
        if not database_exist:
            break

    __logger.info("try to create database:%s", database_name)
    response = mochow_client.create_database(database_name)
    __logger.info("create database response:%s", response)
    ######################################################################################################
    #               table operation examples
    ######################################################################################################
    while True:
        table_name = "table_test_" + generate_random_string(5);
        response = mochow_client.list_tables(database_name)
        __logger.info("list tables:%s", response)
        table_exist = False
        for table in response.tables:
            if (table == table_name):
                table_exist = True
                break
        
        if not table_exist:
            break
    
    tablet = 10
    partition = {
        "partitionType": "HASH",
        "partitionNum": tablet
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
    __logger.info("try to create table:%s", table_name)
    response = mochow_client.create_table(database_name, table_name,
            partition=partition, fields=fields, indexes=indexes, replication=3)
    __logger.info("create table response:%s", response)
    
    while True:
        response = mochow_client.desc_table(database_name, table_name)
        __logger.info("desc table:%s %s", table_name, response)
        time.sleep(10)
        if response.table['state'] == u"NORMAL":
            break
    ######################################################################################################
    #               row operation examples
    ######################################################################################################
    insert_rows = tablet * 200
    ## insert row
    for i in range(insert_rows):
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
        __logger.info("insert row response:%s", response)
    
    # query row
    for i in range(100):
        j = random.randint(0, tablet - 1)
        #j = 0
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
        __logger.info("query row response:%s", response)

    ######################################################################################################
    #               rebuild vector index
    ######################################################################################################
    response = mochow_client.rebuild_vector_index(database_name, table_name, "vector_idx")
    __logger.info("rebuild vector index response: %s", response)

    while True:
        response = mochow_client.desc_index(database_name, table_name, "vector_idx")
        __logger.info("desc index response%s", response)
        time.sleep(10)
        if response.index['state'] == u"NORMAL":
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
    __logger.info("search row response: %s", response)
    ######################################################################################################
    #               search after drop index
    ######################################################################################################
    response = mochow_client.drop_index(database_name, table_name, "vector_idx")
    __logger.info("drop index response: %s", response)
        
    try:
        response = mochow_client.desc_index(database_name, table_name, "vector_idx")
        __logger.info("desc index response: %s", response)
    except Exception as e:
        __logger.info("desc index response: %s", e)
    
    response = mochow_client.search_row(database_name, table_name, anns)
    __logger.info("search row response: %s", response)

