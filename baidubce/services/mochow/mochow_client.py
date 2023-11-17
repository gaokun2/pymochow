"""
This module provides a client class for Mochow
"""

import copy
import http.client
import json
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
                body=json.dumps({'database': database_name}),
                config=config)
    
    def drop_database(self, database_name, config=None):
        """
        drop database
        """
        return self._send_request(http_methods.DELETE,
                resource="database",
                params={b'database': database_name})

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
            body_parser=None
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
                    config, bce_v1_signer.sign, [handler.parse_error, body_parser],
                    http_method, path, body, headers, params)
        except BceHttpClientError as e:
            raise e

