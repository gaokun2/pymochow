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


with io.open(os.path.join("baidubce", "__init__.py"), "rt") as f:
    SDK_VERSION = re.search(r"SDK_VERSION = b'(.*?)'", f.read()).group(1)

setup(
    name='baidu-mochow-sdk',
    version=SDK_VERSION,
    install_requires=['pycryptodome>=3.8.0',
                      'future>=0.6.0',
                      'six>=1.4.0'],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',
    packages=['baidubce',
              'baidubce.auth',
              'baidubce.http',
              'baidubce.retry',
              'baidubce.services',
              'baidubce.services.mochow'
              ],
    url='http://bce.baidu.com',
    license='Apache License 2.0',
    author='',
    author_email='',
    description='Mochow SDK for python'
)
