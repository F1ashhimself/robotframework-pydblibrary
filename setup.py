#    Copyright (c) 2013-2014 Mirantis, Inc.
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

from setuptools import setup
from os.path import join, dirname

execfile(join(dirname(__file__), 'src', 'Pydblibrary', 'version.py'))


setup(
    name='robotframework-pydblibrary',
    version=VERSION,
    author='Max Beloborodko',
    author_email='f1ashhimself@gmail.com',
    url='https://github.com/F1ashhimself/robotframework-pydblibrary',
    license='Apache License 2.0',
    description='Python Database utility library for Robot Framework',
    long_description=open('README.md').read(),
    package_dir={'': 'src'},
    packages=['Pydblibrary', 'Pydblibrary.keywords'],
    test_suite='test',
    install_requires=['robotframework>=2.8.1'],
    platforms='any',
    zip_safe=False
)
