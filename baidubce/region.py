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
This module provides constants that define BCE Regions.
"""

from builtins import str
from builtins import bytes

class Region(object):
    """
    
    :param object:
    :return:
    """
    def __init__(self, *region_id_list):
        """初始化 RegionIdList 实例
        
        Args:
            *region_id_list (int): 待添加到 RegionIdList 中的区域ID列表
        
        Returns:
            None: 无返回值
        """
        self.region_id_list = region_id_list

BEIJING = Region('bj')
