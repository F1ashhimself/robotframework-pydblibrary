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
    """
    Class that handles all keywords from group 'Execution'.
    """

    def query(self, selectStatement):
        """
        Performs query.

        *Arguments:*
            - selectStatement: string, sql select statement.

        *Return:*
            - Fetched result of performed query.

        *Examples:*
        | @{queryResults} | Query | select * from employee |
        """

        cur = self._execute_sql(selectStatement)
        result = cur.fetchall()

        return result

    def description(self, selectStatement):
        """
        Gets description of query.

        *Arguments:*
            - selectStatement: string, sql select statement.

        *Return:*
            - Description of performed query.

        *Examples:*
        | @{queryDescription} | Description | select * from employee |
        """

        cur = self._execute_sql(selectStatement)
        description = cur.description

        return description

    def execute_sql(self, sqlStatement):
        """
        Executes sql and commits it.

        *Arguments:*
            - sqlStatement: string, sql statement.

        *Return:*
            - None

        *Examples:*
        | Execute Sql | update name from employee where id=7 |
        """

        self._execute_sql(sqlStatement, True)

    def read_single_value_from_table(self, tableName, columnName, whereClause):
        """
        Reads single value from table.
        If there will be more than one row that satisfies performed query, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name.
            - columnName: string, column name or names divided by comma.

        *Return:*
            - Fetched single value.

        *Examples:*
        | @{queryResult} | Read Single Value From Table | employee \
        | name, surname | age=27 |
        """

        sqlStatement = 'SELECT %s FROM %s WHERE %s' % (columnName, tableName,
                                                       whereClause)

        result = self.query(sqlStatement)

        assert len(result) == 1, \
            ("Expected to have 1 row from '%s' "
                "but got %s rows : %s." % (sqlStatement, len(result), result))
        logger.debug("Got 1 row from %s: %s." % (sqlStatement, result))

        return result[0]

    def db_elements_are_equal(self, selectStatement, firstAliasOrIndex,
                              secondAliasOrIndex):
        """
        Checks that fetched results of performed query on two
        databases are equal.
        If DB elements will not be equal, then this will
        throw an AssertionError.

        *Arguments:*
            - selectStatement: string, sql select statement.
            - firstAliasOrIndex: string or int, alias or index of \
            first database.
            - secondAliasOrIndex: string or int, alias or index of \
            second database.

        *Return:*
            - None

        *Examples:*
        | DB Elements Are Equal | select name, surname from employee | \
        SomeCompanyDB1 | SomeCompanyDB1 |
        """

        result = self._get_elements_from_two_db(selectStatement,
                                                firstAliasOrIndex,
                                                secondAliasOrIndex)

        assert set(result[0]) == set(result[1]), \
            "Expected to have equal elements but '%s' is not equal to '%s'" \
            % (result[0], result[1])
        logger.debug("Results fetched from %s on %s and %s databases are"
                     " equal." % (selectStatement, firstAliasOrIndex,
                                  secondAliasOrIndex))

    def db_elements_are_not_equal(self, selectStatement, firstAliasOrIndex,
                                  secondAliasOrIndex):
        """
        Checks that fetched results of performed query on two
        databases are not equal.
        If DB elements will be equal, then this will
        throw an AssertionError.

        *Arguments:*
            - selectStatement: string, sql select statement.
            - firstAliasOrIndex: string or int, alias or index of \
            first database.
            - secondAliasOrIndex: string or int, alias or index of \
            second database.

        *Return:*
            - None

        *Examples:*
        | DB Elements Are Not Equal | select name, surname from employee | \
        SomeCompanyDB1 | SomeCompanyDB1 |
        """

        result = self._get_elements_from_two_db(selectStatement,
                                                firstAliasOrIndex,
                                                secondAliasOrIndex)

        assert set(result[0]) != set(result[1]), \
            ("Expected to have not equal elements but they equal. "
             "Query result:'%s'" % (result[0]))
        logger.debug("Results fetched from %s on %s and %s databases are"
                     " not equal." % (selectStatement, firstAliasOrIndex,
                                      secondAliasOrIndex))

    def _get_elements_from_two_db(self, selectStatement, firstAliasOrIndex,
                                  secondAliasOrIndex):
        """
        Gets fetched results of performed query on two databases.

        *Arguments:*
            - selectStatement: string, sql select statement.
            - firstAliasOrIndex: string or int, alias or index of \
            first database.
            - secondAliasOrIndex: string or int, alias or index of \
            second database.

        *Return:*
            - tuple of two results.
        """

        currentDbIndex = self._connectionCache.current_index

        self.set_current_database(firstAliasOrIndex)
        firstResult = self.query(selectStatement)

        self.set_current_database(secondAliasOrIndex)
        secondResult = self.query(selectStatement)

        self.set_current_database(currentDbIndex)  # Back to initial database.

        return firstResult, secondResult
