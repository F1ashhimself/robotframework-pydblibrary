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


class DBDriverMock(object):
    connection = None

    def connect(self, database, user, password, host, port):
        if not self.connection:
            connection = DBConnectionMock()
            self.connection = connection

        return self.connection


class _Reverter(object):

    def _returnAndRevertIf(self, value, revertNeededIf):
        result = value
        if revertNeededIf:
            value = not value

        return result


class DBConnectionMock(_Reverter):

    def __init__(self):
        self._cursor = Cursor()
        self._flags = {'connected': True,
                       'committed': False}

    @property
    def isConnected(self):
        return self._flags['connected']

    @property
    def wasCommitted(self):
        return self._returnAndRevertIf(self._flags['committed'], True)

    def cursor(self):
        return self._cursor

    def commit(self):
        self._flags['committed'] = True

    def rollback(self):
        pass

    def close(self):
        self._flags['connected'] = False


class Cursor(_Reverter):

    def __init__(self):
        self._executedFlag = False
        self._queryResponses = {}
        self._lastExecutedCommand = None

    def setQueryResponses(self, queryResponses):
        self._queryResponses = {key.upper(): value for key, value in
                                queryResponses.iteritems()}

    @property
    def wasExecuted(self):
        return self._returnAndRevertIf(self._executedFlag, True)

    def execute(self, sqlStatement):
        self._executedFlag = True
        self._lastExecutedCommand = sqlStatement.upper()

    def fetchall(self):
        if self._lastExecutedCommand in self._queryResponses.keys():
            return self._queryResponses[self._lastExecutedCommand]

        return []

    @property
    def description(self):
        raise NotImplementedError

    @property
    def rowcount(self):
        return len(self.fetchall())
