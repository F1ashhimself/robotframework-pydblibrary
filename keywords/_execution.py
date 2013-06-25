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

from robot.api import logger
from _common import _CommonActions


class _ExecutionKeywords(_CommonActions):

    def query(self, selectStatement):
        cur = self._execute_sql(selectStatement)
        result = cur.fetchall()

        return result

    def description(self, selectStatement):
        cur = self._execute_sql(selectStatement)
        description = cur.description

        return description

    def execute_sql(self, sqlStatement):
        self._execute_sql(sqlStatement, True)

    def read_single_value_from_table(self, tableName, columnName, whereClause):
        sqlStatement = 'SELECT %s FROM %s WHERE %s' % (columnName, tableName,
                                                       whereClause)

        result = self.query(sqlStatement)

        assert len(result) == 1, \
            ("Expected to have 1 row from '%s' "
                "but got %s rows : %s." % (sqlStatement, len(result), result))

        return result
