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


class _RowsKeywords(_CommonActions):
    """
    Class that handles all keywords from group 'Rows'.
    """

    def rows_count(self, selectStatement):
        """
        Returns the number of rows fetched using 'selectStatement'.

        *Arguments:*
            - selectStatement: string, SQL query.

        *Return:*
            - int, rows count.

        *Examples:*
        | ${rows_count} | Rows Count | select * from TableName |
        """
        cur = self._execute_sql(selectStatement)
        return cur.rowcount

    def rows_count_is_0(self, selectStatement):
        """
        Verifies that 'selectStatement' query does not fetch any row.
        I.e. row count equals to 0.
        If the number of rows is greater than 0, \
        then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, SQL query.

        *Return:*
            - None.

        *Examples:*
        | Rows Count Is 0 | select * from TableName where name = 'Peter' |
        """
        count = self.rows_count(selectStatement)
        assert not count, ("Expected to have 0 rows from '%s', "
                           "but got %s rows." % (selectStatement, count))
        logger.debug("Got 0 rows from %s." % selectStatement)

    def row_count_is_equal_to_x(self, selectStatement, numRows):
        """
        Verifies that number of rows fetched using 'selectStatement'
        equals to 'numRows' value.
        If the number of rows does not equal to the 'numRows' value, \
        then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, SQL query;
            - numRows: int, expected number of rows.

        *Return:*
            - None.

        *Examples:*
        | Row Count Is Equal To X | select * from TableName | 125 |
        """
        count = self.rows_count(selectStatement)
        assert count == numRows, ("Expected to have %s rows from '%s', but "
                                  "got %s rows." % (numRows, selectStatement,
                                                    count))
        logger.debug("Got %s rows from %s." % (numRows, selectStatement))

    def row_count_is_greater_than_x(self, selectStatement, numRows):
        """
        Verifies that number of rows fetched using 'selectStatement'
        is greater than 'numRows' value.
        If the number of rows equals to the 'numRows' value or is less
        than it, then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, SQL query;
            - numRows: int, minimal number of rows.

        *Return:*
            - None.

        *Examples:*
        | Row Count Is Greater Than X | select * from TableName | 125 |
        """
        count = self.rows_count(selectStatement)
        assert count > numRows, ("Expected to have greater than %s rows from "
                                 "'%s', but got %s rows." % (numRows,
                                                             selectStatement,
                                                             count))
        logger.debug("Got %s rows from %s." % (numRows, selectStatement))

    def row_count_is_less_than_x(self, selectStatement, numRows):
        """
        Verifies that number of rows fetched using 'selectStatement'
        is less than 'numRows' value.
        If the number of rows equals to the 'numRows' value or is greater
        than it, then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, SQL query;
            - numRows: int, maximal number of rows.

        *Return:*
            - None.

        *Examples:*
        | Row Count Is Less Than X | select * from TableName | 125 |
        """
        count = self.rows_count(selectStatement)
        assert count < numRows, ("Expected to have less than %s rows from "
                                 "'%s', but got %s rows." % (numRows,
                                                             selectStatement,
                                                             count))
        logger.debug("Got %s rows from %s." % (numRows, selectStatement))

    def delete_all_rows_from_table(self, tableName):
        """
        Deletes everything from the table with given name.

        *Arguments:*
            - tableName: string, table name.

        *Return:*
            - None.

        *Examples:*
        | Delete All Rows From Table | TableName |
        """
        self._execute_sql("delete from %s" % tableName, True)
        logger.info("All rows are deleted from table '%s'." % tableName)

    def check_content_for_row_identified_by_rownum(self, colNames,
                                                   expectedValues, tableName,
                                                   rowNumValue):
        """
        Fetches given columns from the table with given name.
        Verifies that received content in the row with number rowNumValue
        equal to the given expected values.
        If expected content will be not equal to actual, \
        then this will throw an AssertionError.

        *Arguments:*
            - colNames: list, column names to be retrieved from DB;
            - expectedValues: list, expected fields values;
            - tableName: string, table name;
            - rowNumValue: int, number of row to be checked.

        *Return:*
            - None.

        *Examples:*
        | Check Content For Row Identified By Rownum | name,surname | 'John', \
        'Doe', | TableName | 50 |
        """
        assert len(colNames) == len(expectedValues),\
            "'colNames' and 'expectedValues' should have the same length."

        selectStatement = "select %s from %s" % (",".join(colNames), tableName)
        res = self.query(selectStatement)

        assert len(res) >= rowNumValue, ("Row %s does not exist for statement "
                                         "%s" % (rowNumValue, selectStatement))
        actualValues = res[rowNumValue - 1]

        result = []
        for i, col in enumerate(colNames):
            if expectedValues[i] != actualValues[i]:
                result.append((col, expectedValues[i], actualValues[i]))

        assert not result, ('\n'.join(["Expected that '%s' from row %s equals "
                                       "to '%s', but got '%s'." %
                                       (n, rowNumValue, e, a) for n, e, a in
                                       result]))
        logger.debug("Content for row %s equals to the expected values %s." %
                     (rowNumValue, expectedValues))

    def check_content_for_row_identified_by_where_clause(self, colNames,
                                                         expectedValues,
                                                         tableName, where):
        """
        Fetches given columns from the 'tableName' table using where-clause.
        Where-clause should uniquely identify only one row.
        Verifies that content in the received row equals to the given
        expected values.
        If expected content will be not equal to actual, then this will throw
        an AssertionError.

        *Arguments:*
            - colNames: list, column names to be retrieved from DB;
            - expectedValues: list, expected fields values;
            - tableName: string, table name;
            - where: string, where-clause.

        *Return:*
            - None.

        *Examples:*
        | Check Content For Row Identified By Where Clause | id,surname | 50, \
        'Doe', | TableName | name = 'John' |
        """
        assert len(colNames) == len(expectedValues),\
            "'colNames' and 'expectedValues' should have the same length."

        actualValues = \
            self.read_single_value_from_table(tableName, ",".join(colNames),
                                              where)
        result = []
        for i, col in enumerate(colNames):
            if expectedValues[i] != actualValues[i]:
                result.append((col, expectedValues[i], actualValues[i]))

        assert not result, ('\n'.join(["Expected that '%s' equals to '%s', "
                                       "but got '%s'." % (n, e, a)
                                       for n, e, a in result]))
        logger.debug("Content for row which corresponds to the 'where %s'"
                     "statement equals to the expected values %s." %
                     (where, expectedValues))

    def verify_number_of_rows_matching_where(self, tableName, where,
                                             rowNumValue):
        """
        Fetches rows using given where-clause from the table with given
        'tableName'.
        Verifies that number of rows equals to the given rowNumValue,
        otherwise AssertionError is thrown.
        If number of rows will be not equal to actual, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name;
            - where: string, where-clause;
            - rowNumValue: int, expected number of rows.

        *Return:*
            - None.

        *Examples:*
        | Verify Number Of Rows Matching Where | TableName | name='John' | 12 |
        """
        selectStatement = "select * from %s where %s" % (tableName, where)
        count = self.rows_count(selectStatement)

        assert count == rowNumValue, ("Expected to get %s row(s) for where-"
                                      "clause statement '%s', but got %s." %
                                      (rowNumValue, where, count))
        logger.debug("Number of rows matching 'where %s' statement equals to"
                     "%s." % (where, rowNumValue))

    def row_should_not_exist_in_table(self, tableName, where):
        """
        Verifies that table with given 'tableName' doesn't have rows
        matching given where-clause statement.
        If the number of rows is greater than 0, \
        then this will throw an AssertionError.

        *Arguments:*
            - tableName: string, table name
            - where: string, where-clause.

        *Return:*
            - None.

        *Examples:*
        | Row Should Not Exist In Table | TableName | surname = 'Doe' |
        """
        selectStatement = "select * from %s where %s" % (tableName, where)
        actualValues = self.query(selectStatement)

        assert len(actualValues) == 0, ("Expected to get 0 rows for where-"
                                        "clause statement '%s', but got %s: "
                                        "%s." % (where, len(actualValues),
                                                 actualValues))
        logger .debug("There is no rows matching 'where %s' statement." %
                      where)
