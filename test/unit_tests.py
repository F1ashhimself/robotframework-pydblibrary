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

import sys
from os.path import join, dirname
from unittest import TestCase, main
from dbmock import DBDriverMock
sys.path.append(join(dirname(dirname(__file__)), 'src'))
from Pydblibrary import Pydblibrary


class PydblibraryTests(TestCase):

    def testOpenConnection(self):
        dbDriver = DBDriverMock()
        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        self.assertTrue(dbDriver.connection.isConnected,
                        'Disconnected from database but should be connected.')

    def testCloseConnection(self):
        dbDriver = DBDriverMock()
        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.disconnect_from_database()

        self.assertFalse(dbDriver.connection.isConnected,
                         'Connected to database but should be disconnected.')

    def testCloseAllConnections(self):
        dbDriver1 = DBDriverMock()
        dbDriver2 = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver1, 'someDbName1', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.connect_to_database(dbDriver2, 'someDbName2', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.disconnect_from_all_databases()

        self.assertFalse(dbDriver1.connection.isConnected or
                         dbDriver2.connection.isConnected,
                         'Connected to database but should be disconnected.')

    def testSetCurrentDatabase(self):
        dbDriver1 = DBDriverMock()
        dbDriver2 = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver1, 'someDbName1', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db1')

        sut.connect_to_database(dbDriver2, 'someDbName2', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db2')

        self.assertTrue(id(sut._connectionCache.current.connection) ==
                        id(dbDriver2.connection),
                        'Current database is db1 but should be db2.')

        sut.set_current_database('db1')

        self.assertTrue(id(sut._connectionCache.current.connection) ==
                        id(dbDriver1.connection),
                        'Current database is db2 but should be db1.')

    def testQuery(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        result = sut.query('select * from employee')

        self.assertTrue(responses['select * from employee'] == result,
                        'Query was performed with fail.')

    def testExecuteSql(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        self.assertFalse(dbDriver.connection.cursor().wasExecuted or
                         dbDriver.connection.wasCommitted,
                         'Incorrect driver initial state.')

        sut.execute_sql('select 1 from 1')

        self.assertTrue(dbDriver.connection.cursor().wasExecuted and
                        dbDriver.connection.wasCommitted,
                        'Query was not performed and committed.')

    def testReadSingleValueFromTable(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select name from employee where surname='Doe'":
                         [('John',)]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        result = sut.read_single_value_from_table('employee', 'name',
                                                  "surname='Doe'")

        self.assertTrue(responses[("select name from employee "
                                   "where surname='Doe'")][0] ==
                        result, 'Read Single Value was performed with fail.')

    def testDbElementsAreEqual(self):
        dbDriver1 = DBDriverMock()
        dbDriver2 = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver1, 'someDbName1', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db1')

        sut.connect_to_database(dbDriver2, 'someDbName2', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db2')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver1.connection.cursor().setQueryResponses(responses)
        dbDriver2.connection.cursor().setQueryResponses(responses)

        sut.db_elements_are_equal('select * from employee', 'db1', 'db2')

    def testDbElementsAreNotEqual(self):
        dbDriver1 = DBDriverMock()
        dbDriver2 = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver1, 'someDbName1', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db1')

        sut.connect_to_database(dbDriver2, 'someDbName2', 'someUsername',
                                'somePassword', 'someHost', '7777', 'db2')

        responses1 = {'select * from employee': [(0, 'John', 'Doe'),
                                                 (1, 'Jane', 'Doe')]}

        responses2 = {'select * from employee': [(0, 'John', 'Doe')]}

        dbDriver1.connection.cursor().setQueryResponses(responses1)
        dbDriver2.connection.cursor().setQueryResponses(responses2)

        sut.db_elements_are_not_equal('select * from employee', 'db1', 'db2')

    def testCheckIfExistsInDatabase(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.check_if_exists_in_database('select * from employee')

    def testCheckIfNotExistsInDatabase(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.check_if_not_exists_in_database('select * from employee')

    def testRowsCount(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        self.assertTrue(sut.rows_count('select * from employee') == 2,
                        'Incorrect rows count.')

    def testRowsCountIsZero(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.rows_count_is_0('select * from employee')

    def testRowCountIsEqualToX(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.row_count_is_equal_to_x('select * from employee', 2)

    def testRowCountIsGreaterThanX(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.row_count_is_greater_than_x('select * from employee', 1)

    def testRowCountIsLessThanX(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select * from employee': [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.row_count_is_less_than_x('select * from employee', 3)

    def testCheckContentForRowIdentifiedByRownum(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {'select id,name,surname from employee':
                     [(0, 'John', 'Doe'), (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.check_content_for_row_identified_by_rownum(
            ['id', 'name', 'surname'], [0, 'John', 'Doe'], 'employee', 1)

    def testCheckContentForRowIdentifiedByWhereClause(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select id,name,surname from employee where name='John'":
                     [(0, 'John', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.check_content_for_row_identified_by_where_clause(
            ['id', 'name', 'surname'], [0, 'John', 'Doe'],
            'employee', "name='John'")

    def testVerifyNumberOfRowsMatchingWhere(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select * from employee where surname='Doe'":
                     [(0, 'John', 'Doe'),
                      (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.verify_number_of_rows_matching_where('employee',
                                                 "surname='Doe'", 2)

    def testRowShouldNotExistInTable(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.row_should_not_exist_in_table('employee', "surname='Doe'")

    def testTableMustExist(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {("SELECT * FROM information_schema.tables WHERE "
                      "table_name='employee'"):
                     [('someDbName', 'public', 'employee', 'BASE TABLE',
                       None, None, None, None, None, 'YES', 'NO', None)]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.table_must_exist('employee')

    def testTableMustBeEmpty(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        sut.table_must_be_empty('employee')

    def testTableMustContainLessThanNumberOfRows(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select * from employee": [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.table_must_contain_less_than_number_of_rows('employee', 3)

    def testTableMustContainMoreThanNumberOfRows(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select * from employee": [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.table_must_contain_more_than_number_of_rows('employee', 1)

    def testTableMustContainNumberOfRows(self):
        dbDriver = DBDriverMock()

        sut = Pydblibrary()

        sut.connect_to_database(dbDriver, 'someDbName', 'someUsername',
                                'somePassword', 'someHost', '7777')

        responses = {"select * from employee": [(0, 'John', 'Doe'),
                                                (1, 'Jane', 'Doe')]}

        dbDriver.connection.cursor().setQueryResponses(responses)

        sut.table_must_contain_number_of_rows('employee', 2)

if __name__ == '__main__':
    main()
