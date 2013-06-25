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

    def check_if_exists_in_database(self, selectStatement):
        assert self.query(selectStatement), \
            ("Expected to have at least one row from '%s' "
                "but got 0 rows." % selectStatement)

    def check_if_not_exists_in_database(self, selectStatement):
        queryResults = self.query(selectStatement)
        assert not queryResults, \
            ("Expected to have no rows from '%s' "
                "but got some rows : %s." % (selectStatement, queryResults))
