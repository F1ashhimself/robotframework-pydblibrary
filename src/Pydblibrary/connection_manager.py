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
    """
    Connection class that could handle driver connection and driver name
    """

    def __init__(self, driverName, dbConnection):
        self.driverName = driverName
        self.connection = dbConnection

    def close(self):
        """
        Close database connection.
        """

        self.connection.close()


class ConnectionManager(object):
    """
    Class that handles connection/disconnection to databases.
    """

    def __init__(self):
        self._connectionCache = ConnectionCache()

    def connect_to_database(self, driverName=None, dbName=None, username=None,
                            password=None, host='localhost',
                            port="5432", alias=None):
        """
        Connects to database.

        *Arguments:*
            - driverName: string, name of python database driver.
            - dbName: string, name of database.
            - username: string, name of user.
            - password: string, user password.
            - host: string, database host.
            - port: int, database port.
            - alias: string, database alias for future use.

        *Return:*
            - None

        *Examples:*
        | Connect To Database | psycopg2 | PyDB | username | password \
        | localhost | 5432 | SomeCompanyDB |
        """

        if isinstance(driverName, str):
            dbModule = __import__(driverName)
        else:
            dbModule = driverName
            driverName = 'Mock DB Driver'

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
        logger.info("Established connection to the %s database. "
                    "Alias %s. Driver name: %s." % (dbName, alias, driverName))

    def disconnect_from_database(self):
        """
        Disconnects from database.

        *Arguments:*
            - None

        *Return:*
            - None

        *Examples:*
        | Disconnect From Database |
        """

        if self._connectionCache.current:
            self._connectionCache.current.close()

            curIndex = self._connectionCache.current_index
            aliasesCache = self._connectionCache._aliases
            if curIndex in aliasesCache.values():
                keyForDeletion = \
                    aliasesCache.keys()[aliasesCache.values().index(curIndex)]
                del self._connectionCache._aliases[keyForDeletion]

            self._connectionCache.current = self._connectionCache._no_current
            logger.info("Current database was disconnected.")
            cls_attr = getattr(type(self._connectionCache),
                               'current_index', None)
            if isinstance(cls_attr, property) and cls_attr.fset is not None:
                self._connectionCache.current_index = None

    def disconnect_from_all_databases(self):
        """
        Disconnects from all previously opened databases.

        *Arguments:*
            - None

        *Return:*
            - None

        *Examples:*
        | Disconnect From All Databases |
        """

        self._connectionCache.close_all('close')
        logger.info("All databases were disconnected.")

    def set_current_database(self, aliasOrIndex):
        """
        Sets current database by alias or index.

        *Arguments:*
            - aliasOrIndex: int or string, alias or index of opened database.

        *Return:*
            - None

        *Examples:*
        | # Using index |
        | Set Current Database | 1 |
        | # Using alias |
        | Set Current Database | SomeCompanyDB |
        """

        self._connectionCache.switch(aliasOrIndex)
        logger.info("Connection switched to the %s database." % aliasOrIndex)
