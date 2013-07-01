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
    """
    Class that handles all keywords from group 'Table'.
    """

    def table_must_exist(self, tableName):
        """
        Checks that table exists.
        If table not exist, then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.

        *Return:*
            - None

        *Examples:*
        | Table Must Exist | employee |
        """

        selectStatement = ("SELECT * FROM information_schema.tables WHERE "
                           "table_name='%s'") % tableName

        rowsCount = self.rows_count(selectStatement)
        assert rowsCount, 'Table %s does not exist.' % tableName
        logger.debug("Table %s exists." % tableName)

    def table_must_be_empty(self, tableName):
        """
        Checks that table is empty.
        If table is not empty, then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.

        *Return:*
            - None

        *Examples:*
        | Table Must Be Empty | fired_employee |
        """

        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.rows_count(selectStatement)
        assert not rowsCount, 'Table %s is not empty.' % tableName
        logger.debug("Table %s is empty." % tableName)

    def table_must_contain_less_than_number_of_rows(self, tableName,
                                                    rowsNumber):
        """
        Checks that table contains less number of rows than specified.
        If table contains more or equal number of rows than specified, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.
            - rowsNumber: int, rows number.

        *Return:*
            - None

        *Examples:*
        | Table Must Contain Less Than Number Of Rows | fired_employee | 100 |
        """

        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.rows_count(selectStatement)
        assert rowsCount < rowsNumber,\
            ('Table %s has %s row(s) but should have less than %s row(s).'
                % (tableName, rowsCount, rowsNumber))
        logger.debug("Table %s contains %s row(s)." % (tableName, rowsCount))

    def table_must_contain_more_than_number_of_rows(self, tableName,
                                                    rowsNumber):
        """
        Checks that table contains more number of rows than specified.
        If table contains less or equal number of rows than specified, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.
            - rowsNumber: int, rows number.

        *Return:*
            - None

        *Examples:*
        | Table Must Contain More Than Number Of Rows | employee | 1000 |
        """

        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.rows_count(selectStatement)
        assert rowsCount > rowsNumber,\
            ('Table %s has %s row(s) but should have more than %s row(s).'
                % (tableName, rowsCount, rowsNumber))
        logger.debug("Table %s contains %s row(s)." % (tableName, rowsCount))

    def table_must_contain_number_of_rows(self, tableName, rowsNumber):
        """
        Checks that table contains equal number of rows to specified.
        If table contains less or nire number of rows than specified, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.
            - rowsNumber: int, rows number.

        *Return:*
            - None

        *Examples:*
        | Table Must Contain Number Of Rows | chief_executive_officer | 1 |
        """

        selectStatement = "SELECT * FROM %s" % tableName

        rowsCount = self.rows_count(selectStatement)
        assert rowsCount == rowsNumber,\
            ('Table %s has %s row(s) but should have %s row(s).'
                % (tableName, rowsCount, rowsNumber))
        logger.debug("Table %s contains %s row(s)." % (tableName, rowsCount))

    def get_primary_key_columns_for_table(self, tableName):
        """
        Gets primary key columns for specified table.

        *Note:* This method works only with 'psycopg2' driver.

        *Arguments:*
            - tableName: string, table name.

        *Return:*
            - Primary key columns.

        *Examples:*
        | ${key_columns} | Get Primary Key Columns For Table | employee |
        """

        # // TODO: Extend this method to work with other drivers.

        driverName = self._connectionCache.current.driverName
        assert driverName == 'psycopg2',\
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
        """
        Checks that specified primary key columns equal to table key columns.
        If table contains less or greater number of rows than specified,
        then this will throw an AssertionError.

        *Note:* This method works only with 'psycopg2' driver.

        *Arguments:*
            - tableName: string, table name.
            - columns: list or string divided by comma, table columns.

        *Return:*
            - Primary key columns.

        *Examples:*
        | ${key_columns} | Check Primary Key Columns For Table \
        | id, password_hash |
        """

        tableColumns = self.get_primary_key_columns_for_table(tableName)

        if not isinstance(columns, list):
            columns = [c.strip().lower() for c in columns.split(',')]

        assert set(tableColumns) == set(columns),\
            ('Primary key columns %s in table %s do not match '
             'with specified %s.' % (tableColumns, tableName, columns))
        logger.debug("Primary key columns %s in table %s match with specified"
                     " %s." % (tableColumns, tableName, columns))

    def get_transaction_isolation_level(self):
        """
        Gets transaction isolation level.

        *Note:* This method works only with 'psycopg2' driver.

        *Arguments:*
            - None

        *Return:*
            - Value that contains the name of the transaction isolation level
            Possible return values are: TRANSACTION_READ_UNCOMMITTED, \
            TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, \
            TRANSACTION_SERIALIZABLE or TRANSACTION_NONE.

        *Examples:*
        | ${isolation_level} | Get Transaction Isolation Level |
        """

        # // TODO: Extend this method to work with other drivers.
        driverName = self._connectionCache.current.driverName
        assert driverName == 'psycopg2',\
            "Impossible to use this keyword with '%s' driver." % driverName

        selectStatement = "SELECT CURRENT_SETTING('transaction_isolation');"

        result = 'TRANSACTION_%s' % \
                 self.query(selectStatement)[0][0].replace(' ', '_').upper()
        logger.debug("Transaction isolation level is %s." % result)

        return result

    def transaction_isolation_level_must_be(self, transactionLevel):
        """
        Checks that transaction isolation level is equal to specified.
        If transaction isolation level is not equal to specified, then this \
        will throw an AssertionError.

        *Note:* This method works only with 'psycopg2' driver.

        *Arguments:*
            - transactionLevel: string, transaction isolation level.
            Possible return values are: TRANSACTION_READ_UNCOMMITTED, \
            TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, \
            TRANSACTION_SERIALIZABLE or TRANSACTION_NONE.

        *Return:*
            - None

        *Examples:*
        | ${isolation_level} | Get Transaction Isolation Level |
        """

        result = self.get_transaction_isolation_level()

        assert transactionLevel.upper() == result, \
            ("Expected transaction isolation level '%s' "
                "but current is '%s'." % (transactionLevel.upper(), result))
        logger.debug("Transaction isolation level is %s." % result)
