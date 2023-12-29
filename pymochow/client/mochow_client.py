"""
This module provides a client class for Mochow
"""
import copy
import logging
from typing import List
from requests.adapters import HTTPAdapter

import pymochow
from pymochow.http.http_client import HTTPClient
from pymochow.model.database import Database
from pymochow.exception import ClientError
from pymochow import configuration

_logger = logging.getLogger(__name__)


class MochowClient:
    """
    mochow sdk client
    """
    def __init__(self, config=None, adapter: HTTPAdapter=None):
        self._config = copy.deepcopy(configuration.DEFAULT_CONFIG)
        if config is not None:
            self._config.merge_non_none_values(config)
        self._conn = HTTPClient(config, adapter)
    
    def create_database(self, database_name, config=None) -> Database:
        """create database
        Args:
            database_name(str): database name
            config (dict): config need merge
        Returns:
            Database：database
        """
        db = Database(conn=self._conn, database_name=database_name, config=self._config)
        db.create_database(config)
        return db

    def list_databases(self, config=None) -> List[Database]:
        """list database
        Args:
            config (dict): config need merge
        Returns:
            List[Database]：database list
        """
        db = Database(conn=self._conn, config=self._config)
        return db.list_databases(config)
    
    def database(self, database_name, config=None) -> Database:
        """get database
        Args:
            database_name(str): database name
            config (dict): config
        Returns:
            Database：database
        """
        for db in self.list_databases():
            if db.database_name == database_name:
                return db
        raise ClientError(message='Database not exist: {}'.format(database_name))

    def close(self):
        """Close the connect session."""
        if self._conn:
            self._conn.close()
            self._conn = None
