"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

__author__ = 'Fernando Serena'

from setuptools import setup, find_packages

setup(
    name="rdf2rest",
    version="0.0.1",
    author="Fernando Serena",
    author_email="fernando.serena@centeropenmiddleware.com",
    description="A Linked Data service generator from RDF datasets",
    license="Apache 2",
    keywords=["agora", "linked-data", "rdf"],
    url="https://github.com/fserena/rdf2rest",
    download_url="https://github.com/fserena/rdf2rest/tarball/0.0.1",
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    install_requires=['requests', 'rdflib', 'netifaces', 'flask'],
    classifiers=[],
    scripts=['r2r']
)
