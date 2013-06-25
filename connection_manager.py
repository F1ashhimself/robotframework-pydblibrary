#    Copyright (c) 2013 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from robot.utils import ConnectionCache
from robot.api import logger


class _Connection(object):

    def __init__(self, driverName, dbConnection):
        self.driverName = driverName
        self.connection = dbConnection

    def close(self):
        self.connection.close()


class ConnectionManager(object):

    def __init__(self):
        self._connectionCache = ConnectionCache()

    def connect_to_database(self, driverName=None, dbName=None, username=None,
                            password=None, host='localhost',
                            port="5432", alias=None):
        dbModule = __import__(driverName)

        connParams = {'database': dbName, 'user': username,
                      'password': password, 'host': host, 'port': port}
        if driverName in ("MySQLdb", "pymysql"):
            connParams = {'db': dbName, 'user': username, 'passwd': password,
                          'host': host, 'port': port}
        elif driverName in ("psycopg2"):
            connParams = {'database': dbName, 'user': username,
                          'password': password, 'host': host, 'port': port}

        connStr = ['%s: %s' % (k, str(connParams[k])) for k in connParams]
        logger.debug('Connect using: %s' % ', '.join(connStr))

        dbConnection = _Connection(driverName, dbModule.connect(**connParams))

        self._connectionCache.register(dbConnection, alias)

    def disconnect_from_database(self):
        if self._connectionCache.current:
            self._connectionCache.current.close()
            self._connectionCache.current = self._connectionCache._no_current
            cls_attr = getattr(type(self._connectionCache),
                               'current_index', None)
            if isinstance(cls_attr, property) and cls_attr.fset is not None:
                self._connectionCache.current_index = None

    def disconnect_from_all_databases(self):
        self._connectionCache.close_all('close')

    def set_current_database(self, alias_or_index):
        self._connectionCache.switch(alias_or_index)
