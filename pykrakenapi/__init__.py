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

"""
pykrakenapi - a Python implementation of the Kraken API
=======================================================

Implements the Kraken API methods using the low-level krakenex python
package. See
https://www.kraken.com/help/api
and
https://github.com/veox/python3-krakenex

Whenever convenient, methods return pandas.DataFrame objects. Also implements a
call rate limiter based on your Kraken tier level.


Documentation
-------------

See the docstrings of the methods in the KrakenAPI class.

>>> from pykrakenapi import KrakenAPI
>>> help(KrakenAPI)


Example
-------

>>> import krakenex
>>> from pykrakenapi import KrakenAPI
>>> api = krakenex.API()
>>> k = KrakenAPI(api)
>>> ohlc, last = k.get_ohlc_data("BCHUSD")

>>> print(ohlc)

                           time    open    high     low   close    vwap     volume  count
dtime
2017-11-19 18:36:00  1511116560  1162.0  1174.0  1162.0  1174.0  1174.0   2.210437      1
2017-11-19 18:35:00  1511116500  1175.0  1175.0  1162.0  1162.0  1174.9  13.108000      8
2017-11-19 18:34:00  1511116440  1175.0  1175.0  1175.0  1175.0  1175.0   0.570000      1
2017-11-19 18:33:00  1511116380  1160.7  1160.7  1160.7  1160.7     0.0   0.000000      0
2017-11-19 18:32:00  1511116320  1171.6  1171.6  1160.7  1160.7  1164.5   5.110070     12
...                         ...     ...     ...     ...     ...     ...        ...    ...
2017-11-19 06:41:00  1511073660  1220.8  1220.8  1219.8  1219.8  1220.5   0.100000      3
2017-11-19 06:40:00  1511073600  1221.0  1221.0  1221.0  1221.0  1221.0   0.936837      2
2017-11-19 06:39:00  1511073540  1220.7  1220.7  1220.7  1220.7     0.0   0.000000      0
2017-11-19 06:38:00  1511073480  1217.9  1220.7  1217.6  1220.7  1219.1   1.143000      3
2017-11-19 06:37:00  1511073420  1221.0  1221.0  1217.6  1217.6  1217.7   0.366000      3

[713 rows x 8 columns]

"""

from __future__ import absolute_import

from pykrakenapi.pykrakenapi import KrakenAPI

__all__ = ['KrakenAPI']
__version__ = '0.1.4'
__author__ = "Dominik Traxl <dominik.traxl@posteo.org>"
__copyright__ = "Copyright 2017 Dominik Traxl"
__license__ = "GNU GPL"
__URL__ = "https://github.com/dominiktraxl/pykrakenapi/"

