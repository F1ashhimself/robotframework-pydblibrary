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


class _ExistenceKeywords(_CommonActions):
    """
    Class that handles all keywords from group 'Existence'.
    """

    def check_if_exists_in_database(self, selectStatement):
        """
        Checks that element that satisfies performed query is present in
        database.
        If there will be no elements that satisfies performed query, \
        then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, sql select statement.

        *Return:*
            - None

        *Examples:*
        | Check If Exists In Database | select id from employee where \
        first_name = 'Max' and last_name = 'Beloborodko' |
        """
        queryResults = self.query(selectStatement)
        assert queryResults, ("Expected to have at least one row from '%s' "
                        "but got 0 rows." % selectStatement)
        logger.debug("Got row(s): %s \n from '%s' query." % (queryResults,
                                                             selectStatement))

    def check_if_not_exists_in_database(self, selectStatement):
        """
        Checks that element that satisfies performed query is not present in
        database.
        If there will be elements that satisfies performed query, \
        then this will throw an AssertionError.

        *Arguments:*
            - selectStatement: string, sql select statement.

        *Return:*
            - None

        *Examples:*
        | Check If Not Exists In Database | select id from employee where \
        first_name = 'Osama' and last_name = 'bin Laden' |
        """

        queryResults = self.query(selectStatement)
        assert not queryResults, \
            ("Expected to have no rows from '%s' "
                "but got some rows : %s." % (selectStatement, queryResults))
        logger.debug("Got 0 rows from '%s' query." % selectStatement)
