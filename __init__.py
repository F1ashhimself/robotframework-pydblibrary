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

from connection_manager import ConnectionManager
from keywords import *


class Pydblibrary(ConnectionManager, _ExistenceKeywords, _ExecutionKeywords,
                  _TableKeywords):
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    __version__ = '0.1'

s = Pydblibrary()
s.connect_to_database('psycopg2', 'PyDB', 'postgres', 'pydblibrary',
                      'localhost')

s.transaction_isolation_level_must_be('rr')

s.disconnect_from_all_databases()
