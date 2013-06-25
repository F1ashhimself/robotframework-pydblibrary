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


class _TableKeywords(_CommonActions):

    def table_must_exist(self, tableName):
        selectStatement = "SELECT * FROM information_schema.tables WHERE \
        table_name='%s'" % tableName

        rowsCount = self.row_count(selectStatement)
        assert rowsCount, 'Table %s does not exist.' % tableName

    def table_must_be_empty(self, tableName):
        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.row_count(selectStatement)
        assert rowsCount, 'Table %s is not empty.' % tableName

    def table_must_contain_less_than_number_of_rows(self, tableName,
                                                    rowsNumber):
        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.row_count(selectStatement)
        assert rowsCount < rowsNumber,\
            ('Table %s has %s row(s) but should have less than %s row(s).'
                % (tableName, rowsCount, rowsNumber))

    def table_must_contain_more_than_number_of_rows(self, tableName,
                                                    rowsNumber):
        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.row_count(selectStatement)
        assert rowsCount > rowsNumber,\
            ('Table %s has %s row(s) but should have more than %s row(s).'
                % (tableName, rowsCount, rowsNumber))

    def table_must_contain_number_of_rows(self, tableName, rowsNumber):
        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.row_count(selectStatement)
        assert rowsCount == rowsNumber,\
            ('Table %s has %s row(s) but should have %s row(s).'
                % (tableName, rowsCount, rowsNumber))

    def get_primary_key_columns_for_table(self, tableName):
        # // TODO: Extend this method to work with other drivers.

        driverName = self._connectionCache.current.driverName
        assert driverName == 'psycopg21',\
            "Impossible to use this keyword with '%s' driver." % driverName

        selectStatement = """SELECT KU.table_name as tablename,
        column_name as primarykeycolumn
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        INNER JOIN
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
        TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        and ku.table_name='%s'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;""" % tableName

        primaryKeys = self.query(selectStatement)

        result = []
        for table, columnName in primaryKeys:
            result.append(columnName.lower())

        return result

    def check_primary_key_columns_for_table(self, tableName, columns):
        tableColumns = self.get_primary_key_columns_for_table(tableName)

        if not isinstance(columns, list):
            columns = [c.strip().lower() for c in columns.split(',')]

        assert set(tableColumns) == set(columns),\
            ('Primary key columns %s in table %s do not match '
             'with specified %s.' % (tableColumns, tableName, columns))

    def get_transaction_isolation_level(self):
        # // TODO: Extend this method to work with other drivers.
        driverName = self._connectionCache.current.driverName
        assert driverName == 'psycopg21',\
            "Impossible to use this keyword with '%s' driver." % driverName

        selectStatement = "SELECT CURRENT_SETTING('transaction_isolation');"

        result = 'TRANSACTION_%s' % \
                 self.query(selectStatement)[0][0].replace(' ', '_').upper()

        return result

    def transaction_isolation_level_must_be(self, transactionLevel):
        result = self.get_transaction_isolation_level()

        assert transactionLevel.upper() == result, \
            ("Expected transaction isolation level '%s' "
                "but current is '%s'." % (transactionLevel.upper(), result))
