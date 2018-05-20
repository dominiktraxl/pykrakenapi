# This file is part of pykrakenapi.
#
# pykrakenapi is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# pykrakenapi is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser
# General Public LICENSE along with pykrakenapi. If not, see
# <http://www.gnu.org/licenses/lgpl-3.0.txt> and
# <http://www.gnu.org/licenses/gpl-3.0.txt>.

from setuptools import setup, find_packages

setup(
    name="pykrakenapi",
    version='0.1.4',
    packages=find_packages(),
    author="Dominik Traxl",
    author_email="dominik.traxl@posteo.org",
    url='https://github.com/dominiktraxl/pykrakenapi/',
    download_url='https://github.com/dominiktraxl/pykrakenapi/tarball/v0.1.4',
    description=("A Python implementation of the Kraken API."),
    long_description=open('README.rst').read(),
    python_requires='>=3',
    install_requires=['krakenex>=2.0.0',
                      'pandas'],
    license="GNU GPL",
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    package_data={'pykrakenapi': ['../LICENSE.txt',
                                  '../README.rst']},
)
