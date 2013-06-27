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
from setuptools import setup
from os.path import join, dirname

sys.path.append(join(dirname(__file__), 'src'))

from Pydblibrary import Pydblibrary


setup(
    name='robotframework-pydblibrary',
    version=Pydblibrary.__version__,
    author='Max Beloborodko',
    author_email='f1ashhimself@gmail.com',
    url='https://github.com/F1ashhimself/robotframework-pydblibrary',
    license='Apache License 2.0',
    description='Python Database utility library for Robot Framework',
    long_description=open('README.rst').read(),
    package_dir={'': 'src'},
    packages=['Pydblibrary', 'Pydblibrary.keywords'],
    test_suite='test',
    install_requires=['robotframework>=2.8.1'],
    platforms='any',
    zip_safe=False
)
