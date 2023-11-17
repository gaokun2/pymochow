"""
Configuration for bos samples.
"""

#!/usr/bin/env python
#coding=utf-8

import logging
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials

HOST = 'http://127.0.0.1:9088'
ACCOUNT = 'root'
API_KEY = 'root'

#logger = logging.getLogger('baidubce.services.mochow.mochow_client')
#fh = logging.FileHandler('example.log')
#fh.setLevel(logging.DEBUG)

#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#fh.setFormatter(formatter)
#logger.setLevel(logging.DEBUG)
#logger.addHandler(fh)

config = BceClientConfiguration(credentials=BceCredentials(ACCOUNT, API_KEY), endpoint=HOST)
