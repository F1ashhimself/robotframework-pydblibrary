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
                  _TableKeywords, _RowsKeywords):
    """
    This library supports database-related testing using the Robot
    Framework.

     It allows to establish a connection to a certain database to perform
     tests on the content of certain tables and/or views in that database.

    This is compatible with any Python Database API Specification 2.0 module.

    References:

        - Python Database API Specification 2.0 -
        http://www.python.org/dev/peps/pep-0249/

        - Lists of DB API 2.0 - http://wiki.python.org/moin/DatabaseInterfaces

    Example Usage (with selenium2library):
    | # Setup |
    | Connect To Database | psycopg2 | PyDB | username | password | localhost \
    | 5432 | SomeCompanyDB |
    | Check If Not Exists In Database | select id from employee where \
    first_name = 'Max' and last_name = 'Beloborodko' |
    | # Navigate to Add New Employee page |
    | Go To | http://localhost/employee/AddNew.html | | \
    # Selenium2Library keyword |
    | # Enter first name of new employee |
    | Input Text |  name=first_name | Max | # Selenium2Library keyword |
    | # Enter last name of new employee |
    | Input Text |  name=last_name | Beloborodko | # Selenium2Library keyword |
    | # Save new employee |
    | Click Button | Save | | # Selenium2Library keyword |
    | # Log results |
    | @{queryResults} | Query | select * from employee |
    | Log Many | @{queryResults} |
    | # Verify If Persisted In The Database |
    | Check If Exists In Database | select id from employee where first_name \
    = 'Max' and last_name = 'Beloborodko' |
    | # Teardown |
    | Disconnect From Database |
    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    __version__ = '1.0'
