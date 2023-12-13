"""
This module provides a client class for Mochow
"""

import copy
import http.client
import orjson
import logging

import baidubce
from baidubce import utils
from baidubce.auth import bce_v1_signer
from baidubce.bce_base_client import BceBaseClient
from baidubce.http import bce_http_client
from baidubce.http import http_methods
from baidubce.http import handler
from baidubce.exception import BceHttpClientError
from baidubce.services import mochow

_logger = logging.getLogger(__name__)

class MochowClient(BceBaseClient):
    """
    sdk client
    """
    def __init__(self, config=None):
        """
        """
        BceBaseClient.__init__(self, config)
    
    def connect(self, config=None):
        """connect"""
        config = self._merge_config(config)
        self.conn = bce_http_client.get_connection(config)

    def disconnect(self):
        """disconnect"""
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def list_databases(self, config=None):
        """
        List databases
        """
        path = utils.append_uri(mochow.URL_PREFIX, "database")
        return self._send_request(http_methods.POST, 
                resource="database", 
                params={b'list': b''},
                config=config)

    def create_database(self, database_name, config=None):
        """
        create database
        """
        return self._send_request(http_methods.POST, 
                resource="database", 
                body=orjson.dumps({'database': database_name}),
                params={b'create': b''},
                config=config)
    
    def drop_database(self, database_name, config=None):
        """
        drop database
        """
        return self._send_request(http_methods.DELETE,
                resource="database",
                params={b'database': database_name},
                config=config)

    def create_table(self, database_name, table_name, description=None,
            replication=1, partition=None, fields=None, indexes=None, config=None):
        """
        create table
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["replication"] = replication
        
        if description is not None:
            body["description"] = description
        
        if partition is not None:
            body["partition"] = partition
        
        body["schema"] = {}
        if fields is not None:
            body["schema"]["fields"] = fields
        
        if indexes is not None:
            body["schema"]["indexes"] = indexes

        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))

        return self._send_request(http_methods.POST,
                resource="table",
                params={b'create': b''},
                body=json_body,
                config=config)
    
    def list_tables(self, database_name, config=None):
        """
        list table
        """
        return self._send_request(http_methods.POST,
                resource="table",
                params={b'list': b""},
                body=orjson.dumps({'database': database_name}),
                config=config)

    def drop_table(self, database_name, table_name, config=None):
        """
        drop table
        """
        return self._send_request(http_methods.DELETE,
                resource="table",
                params={
                    b'database': database_name,
                    b'table': table_name},
                config=config)

    def desc_table(self, database_name, table_name, config=None):
        """
        desc table
        """
        return self._send_request(http_methods.POST,
                resource="table",
                params={b'desc': b''},
                body=orjson.dumps({'database': database_name,
                    'table': table_name}),
                config=config)
    
    def add_table_field(self, database_name, table_name, fields, config=None):
        """
        table add field
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["schema"] = {}
        if fields is not None:
            body["schema"]["fields"] = fields
        else:
            raise Exception("param fields not defined")

        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "table",
                body = json_body,
                params={b'addField': b''},
                config=config)

    def insert_row(self, database_name, table_name, rows, config=None, keep_alive=False):
        """
        insert row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        
        if rows is not None:
            body["rows"] = rows
        else:
            raise Exception("param rows not defined")
        
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "row",
                body = json_body,
                params={b'insert': b''},
                config=config,
                keep_alive=keep_alive)
    
    def upsert_row(self, database_name, table_name, rows, config=None, keep_alive=False):
        """
        upsert row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        
        if rows is not None:
            body["rows"] = rows
        else:
            raise Exception("param rows not defined")
        
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource="row",
                body=json_body,
                params={b'upsert': b''},
                config=config,
                keep_alive=keep_alive)

    def delete_row(self, database_name, table_name, primaryKey, partitionKey, config=None):
        """
        delete row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["primaryKey"] = primaryKey
        body["partitionKey"] = partitionKey
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "row",
                body = json_body,
                params={b'delete': b''},
                config=config)

    def update_row(self, database_name, table_name, primaryKey, partitionKey, update, config=None):
        """
        update row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["primaryKey"] = primaryKey
        body["partitionKey"] = partitionKey
        body["update"] = update
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "row",
                body = json_body,
                params={b'update': b''},
                config=config)
    
    def query_row(self, database_name, table_name, primary_key,
            partition_key=None, projections=None, retrieve_vector=False, 
            config=None):
        """
        query row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["primaryKey"] = primary_key
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if projections is not None:
            body["projections"] = projections
        body["retrieveVector"] = retrieve_vector
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "row",
                body = json_body,
                params={b'query': b''},
                config=config)
    
    def search_row(self, database_name, table_name, anns, 
            partition_key=None, projections=None,
            retrieve_vector=False, config=None, keep_alive=False):
        """
        search row
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["anns"] = anns
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if projections is not None:
            body["projections"] = projections
        body["retrieveVector"] = retrieve_vector
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "row",
                body = json_body,
                params={b'search': b''},
                config=config,
                conn=self.conn,
                keep_alive=keep_alive)

    def create_index(self, database_name, table_name, indexes, config=None):
        """
        create index
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["indexes"] = indexes
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "index",
                body = json_body,
                params={b'create': b''},
                config=config)
    
    def modify_index(self, database_name, table_name, index_name, auto_rebuild, 
            config=None):
        """
        modify index
        """
        body = {}
        body["database"] = database_name
        body["table"] = table_name
        body["index"]["indexName"] = index_name
        body["index"]["autoBuild"] = auto_rebuild
        try:
            json_body = orjson.dumps(body)
            _logger.debug("body: {}".format(json_body))
        except Exception as e:
            _logger.debug("e: {}".format(e))
        
        return self._send_request(http_methods.POST,
                resource = "index",
                body = json_body,
                params={b'modify': b''},
                config=config)
    
    def drop_index(self, database_name, table_name, index_name, config=None):
        """
        drop index
        """
        return self._send_request(http_methods.DELETE,
                resource="index",
                params={
                    b'database': database_name,
                    b'table': table_name,
                    b'indexName': index_name},
                config=config)
        
    def rebuild_vector_index(self, database_name, table_name, index_name, config=None):
        """
        rebuild_vector_index
        """
        return self._send_request(http_methods.POST,
                resource="index",
                body=orjson.dumps({"database": database_name,
                    "table": table_name,
                    "indexName": index_name}),
                params={b'rebuild': b''},
                config=config)
    
    def desc_index(self, database_name, table_name, index_name, config=None):
        """
        desc index
        """
        return self._send_request(http_methods.POST,
                resource="index",
                params={b'desc': b''},
                body=orjson.dumps({
                    'database': database_name,
                    'table': table_name,
                    'indexName': index_name
                }),
                config=config)

    def _merge_config(self, config):
        """合并配置。
        
        Args:
            config (dict): 待合并的字典。
        
        Returns:
            dict：已合并的字典。
        
        """
        if config is None:
            return self.config
        else:
            new_config = copy.copy(self.config)
            new_config.merge_non_none_values(config)
            return new_config
    
    @staticmethod
    def _get_uri(config, resource=None):
        """获取资源的URI，包括URL前缀、版本号和资源路径。
        
        Args:
            config (dict): 配置信息，包含URL前缀等信息。
            resource (str): 资源路径。
        
        Returns:
            str: 返回完整的URI字符串。
        
        """
        return utils.append_uri(mochow.URL_PREFIX, mochow.URL_VERSION, resource)

    def _send_request(
            self, http_method, resource=None, body=None, headers=None, params=None,
            config=None,
            body_parser=None,
            conn=None,
            keep_alive=False
            ):
        """
        发送HTTP请求函数
        
        Args:
            http_method (str): HTTP方法，如GET、POST等
            resource (Optional[str]): 请求资源的路径，默认值为None
            body (Any): 请求体内容，默认值为None
            headers (Dict[str, str]): HTTP请求头信息，默认值为None
            params (Dict[str, Any]): URL参数字典，默认值为None
            config (Optional[BceClientConfiguration]): 请求配置信息，默认值为None
            body_parser (Optional[Callable[[bytes], object]]): 返回值解析函数，默认值为None
        
        Returns:
            Union[object, Tuple[object, int]]: 当body_parser为None时返回结果对象，当body_parser不为None时返回元组包含结果对象和状态码
        
        Raises:
            BceHttpClientError: Http客户端异常
            BceClientException: BCE客户端异常
            ValueError: 参数错误
        
        """
        if body_parser is None:
            body_parser = handler.parse_json
       
        path = MochowClient._get_uri(config, resource)

        config = self._merge_config(config)
        _logger.debug("endpoint:{} path:{}".format(config.endpoint, path))
        #if config.security_token is not None:
        #    headers = headers or {}
        #    headers[http_headers.STS_SECURITY_TOKEN] = config.security_token

        try:
            return bce_http_client.send_request(
                    config, conn, bce_v1_signer.sign, 
                    [handler.parse_error, body_parser],
                    http_method, path, body, headers, params,
                    keep_alive=keep_alive)
        except BceHttpClientError as e:
            raise e

