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
This module defines exceptions for BCE.
"""

from baidubce import utils
from builtins import str
from builtins import bytes

class BceError(Exception):
    """Base Error of BCE."""
    def __init__(self, message):
        """
        构造函数
        
        Args:
            message (str): 异常信息
        
        Returns:
            None
        
        """
        Exception.__init__(self, message)


class BceClientError(BceError):
    """Error from BCE client."""
    def __init__(self, message):
        """初始化BceError类及其子类的实例对象。
        
        Args:
            message (str): 错误信息。
        
        """
        BceError.__init__(self, message)


class BceServerError(BceError):
    """Error from BCE servers."""
    REQUEST_EXPIRED = b'RequestExpired'

    """Error threw when connect to server."""
    def __init__(self, message, status_code=None, code=None, request_id=None):
        """
        构造函数，初始化BceRequestError对象并设置其属性。
        
            Args:
                message (str): 错误描述信息。
                status_code (int, optional): HTTP状态码。默认为None。
                code (str, optional): 请求返回的错误代码。默认为None。
                request_id (str, optional): 请求唯一标识符。默认为None。
        
        """
        BceError.__init__(self, message)
        self.status_code = status_code
        self.code = code
        self.request_id = request_id


class BceHttpClientError(BceError):
    """Exception threw after retry"""
    def __init__(self, message, last_error):
        """初始化BceClientError的实例
        
        Args:
            message (str): 错误信息
            last_error (:obj:`Exception`): 上一级的异常对象
        
        """
        BceError.__init__(self, message)
        self.last_error = last_error
