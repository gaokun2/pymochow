# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2023 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
本文件允许模块包以python -m mochow_python_sdk方式直接执行。

Authors: guobo(guobo@baidu.com)
Date:    2023/11/17 11:01:15
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals


import sys
from mochow_python_sdk.cmdline import main
sys.exit(main())
