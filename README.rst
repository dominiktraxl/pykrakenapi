Warning
=======

This repository is not maintained actively at the moment. 

I will, however, respond to issues and integrate code contributions.


pykrakenapi
===========

Implements the Kraken API methods using the low-level krakenex python
package. See

https://www.kraken.com/help/api

and

https://github.com/veox/python3-krakenex

Whenever convenient, methods return pandas.DataFrame objects. Also implements a
call rate limiter based on your Kraken tier level, as well as automatic retries
on HTTPErrors/Kraken API Errors.


Quick Start
-----------

pykrakenapi can be installed via pip from
`PyPI <https://pypi.python.org/pypi/pykrakenapi>`_

::

   $ pip install pykrakenapi

Then, import and get started with::

   >>> from pykrakenapi import KrakenAPI
   >>> help(KrakenAPI)

pykrakenapi requires Python >= 3.3, krakenex >= 2.0.0 and pandas. These
requirements should be installed automatically via pip.


Example
-------

.. code:: python

    import krakenex
    from pykrakenapi import KrakenAPI
    api = krakenex.API()
    k = KrakenAPI(api)
    ohlc, last = k.get_ohlc_data("BCHUSD")
    print(ohlc)


.. parsed-literal::

                               time    open    high     low   close    vwap       volume  count
    dtime
    2017-11-19 18:31:00  1511116260  1175.0  1175.0  1175.0  1175.0     0.0   0.00000000      0
    2017-11-19 18:30:00  1511116200  1175.0  1175.0  1175.0  1175.0     0.0   0.00000000      0
    2017-11-19 18:29:00  1511116140  1175.0  1175.0  1175.0  1175.0  1175.0   0.30000000      1
    2017-11-19 18:28:00  1511116080  1171.2  1175.0  1170.3  1170.3  1174.9  10.02137467      3
    2017-11-19 18:27:00  1511116020  1166.4  1171.2  1166.4  1171.2  1171.2   0.20043000      1
    ...                         ...     ...     ...     ...     ...     ...          ...    ...
    2017-11-19 06:36:00  1511073360  1217.5  1217.5  1217.5  1217.5     0.0   0.00000000      0
    2017-11-19 06:35:00  1511073300  1219.7  1219.7  1217.5  1217.5  1218.8   2.60803000      5
    2017-11-19 06:34:00  1511073240  1221.3  1221.3  1221.3  1221.3     0.0   0.00000000      0
    2017-11-19 06:33:00  1511073180  1220.4  1221.3  1210.7  1221.3  1216.3  17.37500000     11
    2017-11-19 06:32:00  1511073120  1222.0  1222.0  1222.0  1222.0     0.0   0.00000000      0

    [713 rows x 8 columns]


Documentation
-------------

See the docstrings of the methods of the KrakenAPI class.

>>> from pykrakenapi import KrakenAPI
>>> help(KrakenAPI)


FAQ
---

* **Why is my order not executed? Why can't I see my order in Kraken?**

Kraken's API "Add standard order" call enables a validate only feature that
*defaults to True* here. In order to have your order executed, try with
``validate=False``.

.. code:: python

    k.add_standard_order(pair="ATOMXBT", type="buy", ordertype="limit", volume="420.0", price="0.00042", validate=False)


Development
-----------

This package should be considered beta state, since some methods have not been
properly tested yet. Contributions in any way, shape or form are welcome!


Bug Reports
-----------

To search for bugs or report them, please use the bug tracker:
https://github.com/dominiktraxl/pykrakenapi/issues


Licence
-------

Distributed with a `GNU GPL <LICENSE.txt>`_::

    Copyright (C) 2017 pykrakenapi Developers
    Dominik Traxl <dominik.traxl@posteo.org>

