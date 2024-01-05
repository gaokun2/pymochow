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
This module provides authentication functions for bce services.
"""
from __future__ import absolute_import
from builtins import str
from builtins import bytes
import hashlib
import hmac
import logging

from pymochow.http import http_headers
from pymochow import utils
from pymochow import compat

_logger = logging.getLogger(__name__)

def sign(credentials, http_method, path, headers, params,
         timestamp=0, expiration_in_seconds=1800, headers_to_sign=None):
    """
    Create the authorization
    """
    result = b'Bearer account=%s&api_key=%s' % (credentials.account,
            credentials.api_key)
    return result
