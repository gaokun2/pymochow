# Copyright 2014 Baidu, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""
This module provide base class for BCE service clients.
"""
from __future__ import absolute_import
import copy
from builtins import str, bytes

import baidubce
from baidubce import bce_client_configuration
from baidubce.exception import BceClientError
from baidubce.auth import bce_v1_signer
from baidubce.http import handler
from baidubce.http import bce_http_client

class BceBaseClient(object):
    """
    TODO: add docstring
    """
    def __init__(self, config, region_supported=True):
        """
        :param config: the client configuration. The constructor makes a copy of this parameter so
                        that it is safe to change the configuration after then.
        :type config: BceClientConfiguration

        :param region_supported: true if this client supports region.
        :type region_supported: bool
        """
        self.service_id = self._compute_service_id()
        self.region_supported = region_supported
        # just for debug
        self.config = copy.deepcopy(bce_client_configuration.DEFAULT_CONFIG)
        if config is not None:
            self.config.merge_non_none_values(config)
        if self.config.endpoint is None:
            self.config.endpoint = self._compute_endpoint()


    def _compute_service_id(self):
        """计算服务 ID
        
        返回值：
            str: 模块名称中的第二个点前面的部分，即为服务ID
        
        注意：
            该函数只用于计算服务ID，不需要传入任何参数。
        """
        return self.__module__.split('.')[2]

    def _compute_endpoint(self):
        """计算终端点。
        
        Args:
            无参数。
        
        Returns:
            返回字符串类型的终端点。
        """
        if self.config.endpoint:
            return self.config.endpoint
        if self.region_supported:
            return b'%s://%s.%s.%s' % (
                self.config.protocol,
                self.service_id,
                self.config.region,
                baidubce.DEFAULT_SERVICE_DOMAIN)
        else:
            return b'%s://%s.%s' % (
                self.config.protocol,
                self.service_id,
                baidubce.DEFAULT_SERVICE_DOMAIN)

    def _send_request(self, http_method, path, headers=None, params=None, body=None):
        """
        发送HTTP请求
        
        Args:
            http_method (str): HTTP方法，GET/PUT/POST等
            path (str): 请求的路径
            headers (dict, optional): HTTP头部信息，默认为空字典。
            params (dict, optional): URL参数，默认为空字典。
            body (bytes, optional): 请求体内容，默认为空字节数组。
        
        Returns:
            bytes: 返回响应体字节数组。
        
        Raises:
            BceHttpClientError: 网络异常时抛出。
            BceClientError: 服务端返回的状态码非200时抛出。
        
        """
        return bce_http_client.send_request(
            self.config, bce_v1_signer.sign, [handler.parse_error, handler.parse_json],
            http_method, path, body, headers, params)

    def _get_config(self, apiDict, apiName):
        """
        获取API的配置信息
        
        Args:
            apiDict (dict): 包含所有API的字典，格式为 {api_name: config}。
            apiName (str): API名。
        
        Returns:
            dict: 返回对应API的配置信息。
        
        """
        return copy.deepcopy(apiDict[apiName])

    def _add_header(self, apiConfig, key, value):
        """
        将指定键值对添加到API请求头中，如果该键已存在，则会覆盖原有值。
        
        Args:
            apiConfig (dict): API配置信息，需要添加头部信息的字典类型。
            key (str): 要设置的键名。
            value (str): 要设置的值。
        
        Returns:
            None.
        
        """
        self._set_if_nonnull(apiConfig["headers"], key, value)

    def _add_query(self, apiConfig, key, value):
        """
        添加查询参数到API配置中
        
        Args:
            apiConfig (dict): API配置字典，包括名称、方法等信息。
            key (str): 查询参数的键值。
            value (Any): 查询参数的值。
        
        Returns:
            None
        
        """
        # key-only query parameter's value is "" and can satisfy non-null
        self._set_if_nonnull(apiConfig["queries"], key, value)

    def _add_path_param(self, apiConfig, key, value):
        """添加路径参数到 API 配置中
        
        Args:
            apiConfig (dict): API 的配置字典。
            key (str): 参数的键。
            value (Any): 参数的值。
        
        Raises:
            BceClientError: 路径参数值为空时，抛出该异常。
        
        Returns:
            dict: 已更新的 API 配置字典。
        
        """
        if value is None:
            raise BceClientError(b"Path param can't be none.")
        apiConfig["path"] = apiConfig["path"].replace("[" + key + "]", value)

    def _set_if_nonnull(self, params, param_name=None, value=None):
        """设置参数值，如果value不为None则进行设置
        
        Args:
            params (dict): 存储参数值的字典。
            param_name (str): 参数名称。默认值为None。
            value (any): 要设置的参数值。默认值为None。
        
        Returns:
            无返回值，直接在params中设置或删除参数值。
        """
        if value is not None:
            params[param_name] = value