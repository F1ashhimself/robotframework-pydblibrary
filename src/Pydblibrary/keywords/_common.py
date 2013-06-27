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


class _CommonActions(object):
    """
    Class that contains common keywords for all libraries.
    """

    def _execute_sql(self, sqlStatement, commitNeeded=False):
        """
        Executes sql.

        *Arguments:*
            - sqlStatement: string, sql statement.
            - commitNeeded: bool, if True - commit will be performed after
             executing statement.

        *Return:*
            - Database cursor object.
        """

        try:
            cur = self._connectionCache.current.connection.cursor()

            logger.debug("Executing: %s" % sqlStatement)
            cur.execute(sqlStatement)
            if commitNeeded:
                self._connectionCache.current.connection.commit()
            return cur

        finally:
            if cur:
                logger.debug("Rolling back: %s" % sqlStatement)
                self._connectionCache.current.connection.rollback()
