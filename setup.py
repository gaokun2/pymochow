#!/usr/bin/env python
# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2023 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
Setup script.

Authors: guobo(guobo@baidu.com)
Date:    2023/11/17 11:01:15
"""
from __future__ import absolute_import
import io
import os
import re
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with io.open(os.path.join("pymochow", "__init__.py"), "rt") as f:
    SDK_VERSION = re.search(r"SDK_VERSION = b'(.*?)'", f.read()).group(1)

setup(
    name='pymochow',
    version=SDK_VERSION,
    install_requires=['requests',
                      'orjson'],
    python_requires='>=3.7',
    packages=['pymochow',
              'pymochow.auth',
              'pymochow.http',
              'pymochow.retry',
              'pymochow.client',
              'pymochow.model'
              ],
    url='http://bce.baidu.com',
    license='Apache License 2.0',
    author='',
    author_email='',
    description='Mochow SDK for python'
)
