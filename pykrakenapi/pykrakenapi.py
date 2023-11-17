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

"""The core module of pykrakenapi.

This module contains the core class ``KrakenAPI``, implementing the methods
of the official KrakenAPI (https://www.kraken.com/help/api).

For further information type

>>> help(KrakenAPI)

"""

import time
import datetime
from functools import wraps

import pandas as pd

from requests import HTTPError


def crl_sleep(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        self = args[0]
        crl_sleep = self.crl_sleep

        # raise CallRateLimitError if crl sleep is deactivated
        if crl_sleep == 0:
            result = func(*args, **kwargs)
            return result

        # otherwise, retry after "crl_sleep" seconds
        while True:
            try:
                result = func(*args, **kwargs)
                return result
            except CallRateLimitError as err:
                print(err, '\n sleeping for {} seconds'.format(crl_sleep))
                time.sleep(crl_sleep)
                continue

    return wrapper


def callratelimiter(query_type):
    def decorate_func(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            """Call rate limit counter.

            Implementation of a call rate limiter as a decorator. If the call
            rate limit is reached, api calls will be blocked.

            See https://support.kraken.com/hc/en-us/articles/206548367

            """

            self = args[0]

            # public API, with an independent counter system
            if query_type == 'public':
                if self.time_of_last_public_query is not None:
                    now = datetime.datetime.now()
                    lapse = (now - self.time_of_last_public_query).total_seconds()
                    if lapse < 1.0:
                        msg = "public call frequency exceeded (seconds={})"
                        msg = msg.format(str(lapse))
                        raise CallRateLimitError(msg)

                now = datetime.datetime.now()
                self.time_of_last_public_query = now
                # no retries
                if self.retry == 0:
                    result = func(*args, **kwargs)
                    return result
                # do retries
                else:
                    retry = max(self.retry, 1.05)
                    attempt = 0
                    while True:
                        try:
                            result = func(*args, **kwargs)
                            return result
                        except (HTTPError, KrakenAPIError) as err:
                            print('attempt: {} |'.format(
                                str(attempt).zfill(3)), err)
                            attempt += 1
                            time.sleep(retry)
                            now = datetime.datetime.now()
                            self.time_of_last_public_query = now
                            continue

            # private API, determine increment
            if query_type == 'ledger/trade history':
                incr = 2
            elif query_type == 'other':
                incr = 1

            # decrease api counter
            self._decrease_api_counter()

            # return api call
            if self.api_counter < self.limit:
                # no retries
                if self.retry == 0:
                    self.api_counter += incr
                    result = func(*args, **kwargs)
                    return result
                # do retries
                else:
                    attempt = 0
                    while self.api_counter < self.limit:
                        try:
                            self.api_counter += incr
                            result = func(*args, **kwargs)
                            return result
                        except (HTTPError, KrakenAPIError) as err:
                            print('attempt: {} |'.format(
                                str(attempt).zfill(3)), err)
                            attempt += 1
                            time.sleep(self.retry)
                            self._decrease_api_counter()
                            continue

            # raise error if limit exceeded
            msg = ("call rate limiter exceeded (counter={}, limit={})")
            msg = msg.format(str(self.api_counter).zfill(2),
                             str(self.limit).zfill(2))
            raise CallRateLimitError(msg)

        return wrapper
    return decorate_func


class KrakenAPIError(Exception):
    pass


class CallRateLimitError(Exception):
    pass


class KrakenAPI(object):
    """A python implementation of the Kraken API.

    Implements the Kraken API methods using the low-level krakenex python
    package. See
    https://www.kraken.com/help/api
    and
    https://github.com/veox/python3-krakenex

    Parameters
    ----------
    api : krakenex.API
        An instance of the krakenex.API class. A reference to the input
        is created and accessible via ``KrakenAPI.api``.

    tier : str, optional (default='Intermediate')
        Your Kraken tier level, used to adjust the limit of the call rate to
        the Kraken API in order to prevent 15 minute temporary lockouts.
        Must be one of {'None', 'Starter', 'Intermediate', 'Pro'}.
        Set tier='None' to disable the call rate limiter.
        See https://support.kraken.com/hc/en-us/articles/206548367.

    retry : float, optional (default=.5)
        Sleep for ``retry`` seconds after an HTTPError/KrakenAPIError occurred
        and retry the query until it is succesful (or the call rate limiter was
        triggered). If ``retry`` is set to 0, raise a potential
        HTTPError/KrakenAPIError instead of retrying the query.

    crl_sleep : int, optional (default=5)
        Sleep for ``crl_sleep`` seconds after a CallRateLimitError occurred,
        then retry the query. If ``crl_sleep`` is set to 0, raise a potential
        CallRateLimitError instead of sleeping and retrying.

    Attributes
    ----------
    api : krakenex.API
        See Parameters.

    """

    def __init__(self, api, tier='Intermediate', retry=1, crl_sleep=5):

        self.api = api

        # api call rate limiter
        self.time_of_last_public_query = None
        self.time_of_last_query = datetime.datetime.now()

        self.api_counter = 0

        if tier == 'None':
            self.limit = float('inf')
            self.factor = 3  # does not matter

        elif tier == 'Starter':
            self.limit = 15
            self.factor = 3  # down by 1 every three seconds

        elif tier == 'Intermediate':
            self.limit = 20
            self.factor = 2  # down by 1 every two seconds

        elif tier == 'Pro':
            self.limit = 20
            self.factor = 1  # down by 1 every one second

        # retry timers
        self.retry = retry
        self.crl_sleep = crl_sleep

    @crl_sleep
    @callratelimiter('public')
    def get_server_time(self):
        """Get server time.

        This is to aid in approximating the skew time between the server and
        client.

        Returns
        -------
        dt : pandas._libs.tslib.Timestamp
            The server's datetime.
        unixtime : int
            The unix timestamp.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # query
        res = self.api.query_public('Time')

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # extract results
        dt = pd.to_datetime(res['result']['rfc1123'])
        unixtime = res['result']['unixtime']

        return dt, unixtime

    @crl_sleep
    @callratelimiter('public')
    def get_system_status(self):
        """Get system status.

        Return the Kraken system status.

        Returns
        -------
        status : str
            The systems status. Possible status values include:
                online (operational, full trading available)
                cancel_only (existing orders are cancelable, but new orders
                    cannot be created)
                post_only (existing orders are cancelable, and only new post
                    limit orders can be submitted)
                limit_only (existing orders are cancelable, and only new limit
                    orders can be submitted)
                maintenance (system is offline for maintenance)

        timestamp : pandas._libs.tslib.Timestamp
            The server's datetime.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # query
        res = self.api.query_public('SystemStatus')

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # extract results
        status = res['result']['status']
        timestamp = pd.to_datetime(res['result']['timestamp'])

        return status, timestamp

    @crl_sleep
    @callratelimiter('public')
    def get_asset_info(self, info=None, aclass=None, asset=None):
        """Get asset info.

        Return a ``pd.DataFrame`` of asset names and their info.

        Parameters
        ----------
        info : ?, optional (default=None)
            Info to retrieve. If None (default), retrieve all info.

        aclass : str, optional (default=None)
            Asset class. If None (default), aclass='currency'.

        asset : str, optional (default=None)
            Comma delimited list of assets to get info on. If None (default),
            all for given asset class.

        Returns
        -------
        assets : pd.DataFrame
            index = asset name
            aclass = asset class
            altname = alternate name
            decimals = scaling decimal places for record keeping
            display_decimals = scaling decimal places for output display.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('Assets', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        assets = pd.DataFrame(res['result']).T

        return assets

    @crl_sleep
    @callratelimiter('public')
    def get_tradable_asset_pairs(self, info=None, pair=None):
        """Get tradable asset pairs.

        Return a ``pd.DataFrame`` of pair names and their info.

        Parameters
        ----------
        info : str, optional (default=None)
            Info to retrieve. Can be one of {'leverage', 'fees', 'margin'}.
            If None (default), retrieve all info.

        pair : str, optional (default=None)
            Comma delimited list of asset pairs to get info on. If None
            (default), all.

        Returns
        -------
        pairs : pd.DataFrame
            index = pair name
            altname = alternate pair name
            aclass_base = asset class of base component
            base = asset id of base component
            aclass_quote = asset class of quote component
            quote = asset id of quote component
            lot = volume lot size
            pair_decimals = scaling decimal places for pair
            lot_decimals = scaling decimal places for volume
            lot_multiplier = amount to multiply lot volume by to get currency
                volume
            leverage_buy = array of leverage amounts available when buying
            leverage_sell = array of leverage amounts available when selling
            fees = fee schedule array in [volume, percent fee] tuples
            fees_maker = maker fee schedule array in [volume, percent fee]
                tuples (if on maker/taker)
            fee_volume_currency = volume discount currency
            margin_call = margin call level
            margin_stop = stop-out/liquidation margin level

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        If an asset pair is on a maker/taker fee schedule, the taker side is
        given in "fees" and maker side in "fees_maker". For pairs not on
        maker/taker, they will only be given in "fees".

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('AssetPairs', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        pairs = pd.DataFrame(res['result']).T

        return pairs

    @crl_sleep
    @callratelimiter('public')
    def get_ticker_information(self, pair):
        """Get ticker information.

        Return a ``pd.DataFrame`` of pair names and their ticker info.

        Parameters
        ----------
        pair : str
            Comma delimited list of asset pairs to get info on.

        Returns
        -------
        ticker : pd.DataFrame
            index =  pair name
            a = ask array(<price>, <whole lot volume>, <lot volume>),
            b = bid array(<price>, <whole lot volume>, <lot volume>),
            c = last trade closed array(<price>, <lot volume>),
            v = volume array(<today>, <last 24 hours>),
            p = volume weighted average price array(<today>, <last 24 hours>),
            t = number of trades array(<today>, <last 24 hours>),
            l = low array(<today>, <last 24 hours>),
            h = high array(<today>, <last 24 hours>),
            o = today's opening price

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Today's prices start at 00:00:00 UTC.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('Ticker', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        ticker = pd.DataFrame(res['result']).T

        return ticker

    @crl_sleep
    @callratelimiter('public')
    def get_ohlc_data(self, pair, interval=1, since=None, ascending=False):
        """Get ohlc data for a given pair.

        Return a ``pd.DataFrame`` of the OHLC data for a given pair and time
        interval (minutes). Optionally, return data from ``since`` onwards
        (exclusive).

        Parameters
        ----------
        pair : str
            Asset pair to get OHLC data for.

        interval : int, optional (default=1)
            Time frame interval in minutes. Defaults to 1. One of
            {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}.

        since : int, optional (default=None)
            Return committed OHLC data since given unixtime (exclusive). If
            None, retrieve from earliest time possible.

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        ohlc : pd.DataFrame
            index = datetime (UTC)
            time (unixtime)
            open
            high
            low
            close
            vwap
            volume
            count

        last : int
            Unixtime to be used as since when polling for new, committed OHLC
            data.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        The last entry in the OHLC array is for the current, not-yet-committed
        frame and will always be present, regardless of the value of "since".

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('OHLC', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        pair = list(res['result'].keys())[0]
        ohlc = pd.DataFrame(res['result'][pair])
        last = res['result']['last']

        if ohlc.empty:
            return ohlc, last

        else:
            # set time, column names
            ohlc.columns = [
                'time', 'open', 'high', 'low', 'close',
                'vwap', 'volume', 'count',
            ]
            ohlc['dtime'] = pd.to_datetime(ohlc.time, unit='s')
            ohlc.sort_values('dtime', ascending=ascending, inplace=True)
            ohlc.set_index('dtime', inplace=True)
            freq = str(interval) + 'T' if ascending else str(-interval) + 'T'
            ohlc.index.freq = freq

            # dtypes
            for col in ['open', 'high', 'low', 'close', 'vwap', 'volume']:
                ohlc.loc[:, col] = ohlc[col].astype(float)

            return ohlc, last

    @crl_sleep
    @callratelimiter('public')
    def get_order_book(self, pair, count=100, ascending=False):
        """Get order book (market depth).

        Return a ``pd.DataFrame`` for both asks and bids for a given pair.

        Parameters
        ----------
        pair : str
            Asset pair to get market depth for.

        count : int, optional (default=100)
            Maximum number of asks/bids. Per default, get the latest 100
            bids and asks.

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        asks : pd.DataFrame
            The ask side table.
            index = datetime
            price
            volume
            time (unixtime)

        bids : pd.DataFrame
            The bid side table.
            index = datetime
            price
            volume
            time (unixtime)

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('Depth', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        asks = pd.DataFrame(res['result'][pair]['asks'])
        bids = pd.DataFrame(res['result'][pair]['bids'])

        # column names
        cols = ['price', 'volume', 'time']

        if not asks.empty:
            asks.columns = cols
            asks['dtime'] = pd.to_datetime(asks.time, unit='s')
            asks.sort_values('dtime', ascending=ascending, inplace=True)
            asks.set_index('dtime', inplace=True)

        if not bids.empty:
            bids.columns = cols
            bids['dtime'] = pd.to_datetime(bids.time, unit='s')
            bids.sort_values('dtime', ascending=ascending, inplace=True)
            bids.set_index('dtime', inplace=True)

        return asks, bids

    @crl_sleep
    @callratelimiter('public')
    def get_recent_trades(self, pair, since=None, ascending=False):
        """Get recent trades data.

        Return a ``pd.DataFrame`` of recent trade data for a given pair,
        optionally from ``since`` onwards (exclusive).

        Parameters
        ----------
        pair : str
            Asset pair to get trade data for.

        since : int, optional (default=None)
            Return trade data since given unixtime (exclusive). If
            None, retrieve from earliest time possible.

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        trades : pd.DataFrame
            Table containing recent trades for a given pair.
            index = datetime
            price
            volume
            time (unixtime)
            buy/sell
            market/limit
            miscellaneous

        last : int
            Unixtime to be used as since when polling for new trade data.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('Trades', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        pair = list(res['result'].keys())[0]
        trades = pd.DataFrame(res['result'][pair])

        # last timestamp
        last = int(res['result']['last'])

        if not trades.empty:

            trades.columns = [
                'price', 'volume', 'time', 'buy_sell', 'market_limit', 'misc', 'id'
            ]
            trades.buy_sell.replace('b', 'buy', inplace=True)
            trades.buy_sell.replace('s', 'sell', inplace=True)
            trades.market_limit.replace('l', 'limit', inplace=True)
            trades.market_limit.replace('m', 'market', inplace=True)

            # time
            trades['dtime'] = pd.to_datetime(trades.time, unit='s')
            trades.sort_values('dtime', ascending=ascending, inplace=True)
            trades.set_index('dtime', inplace=True)

            # dtypes
            for col in ['price', 'volume']:
                trades.loc[:, col] = trades[col].astype(float)

        return trades, last

    @crl_sleep
    @callratelimiter('public')
    def get_recent_spread_data(self, pair, since=None, ascending=False):
        """Get recent spread data.

        Return a ``pd.DataFrame`` of recent spread data for a given pair,
        optionally from ``since`` onwards (inclusive).

        Parameters
        ----------
        pair : str
            Asset pair to get spread data for.

        since : int, optional (default=None)
            Return spread data since given unixtime (inclusive). If
            None, retrieve from earliest time possible.

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        trades : pd.DataFrame
            Table containing recent spread for a given pair.
            index = datetime
            time (unixtime)
            bid
            ask
            spread (ask - bid)

        last : int
            Unixtime to be used as since when polling for new spread data.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        ``since`` is inclusive so any returned data with the same time as the
        previous set should overwrite all of the previous set's entries at that
        time.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_public('Spread', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        pair = list(res['result'].keys())[0]
        spread = pd.DataFrame(res['result'][pair])

        # last timestamp
        last = int(res['result']['last'])

        if not spread.empty:

            spread.columns = ['time', 'bid', 'ask']

            # time
            spread['dtime'] = pd.to_datetime(spread.time, unit='s')
            spread.sort_values('dtime', ascending=ascending, inplace=True)
            spread.set_index('dtime', inplace=True)

            # spread
            spread.loc[:, 'bid'] = spread.bid.astype(float)
            spread.loc[:, 'ask'] = spread.ask.astype(float)
            spread['spread'] = spread.ask - spread.bid

        return spread, last

    @crl_sleep
    @callratelimiter('other')
    def get_account_balance(self, otp=None):
        """Get asset names and balance amount.

        Return a ``pd.DataFrame`` of asset names and their corresponding
        balance amounts.

        Parameters
        ----------
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        balance : pd.DataFrame
            Table containing asset names and balance amount.
            index = asset name
            vol = balance amount

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Balance', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        balance = pd.DataFrame(index=['vol'], data=res['result']).T

        if not balance.empty:
            balance.loc[:, 'vol'] = balance.vol.astype(float)

        return balance

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_trade_balance(self, aclass='currency', asset='ZEUR', otp=None):
        """Get trade balance info.

        Return a ``pd.DataFrame`` of trade balance info.

        Parameters
        ----------
        aclass : str, optional (default='currency')
            Asset class.

        asset : str, optional (default='ZUSD')
            Base asset used to determine balance.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        tradebalance : pd.DataFrame
            Table containing trade balance info.
            eb = equivalent balance (combined balance of all currencies)
            tb = trade balance (combined balance of all equity currencies)
            m = margin amount of open positions
            n = unrealized net profit/loss of open positions
            c = cost basis of open positions
            v = current floating valuation of open positions
            e = equity = trade balance + unrealized net profit/loss
            mf = free margin = equity - initial margin (maximum margin
                available to open new positions)
            ml = margin level = (equity / initial margin) * 100

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Rates used for the floating valuation is the midpoint of the best bid
        and ask prices.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('TradeBalance', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        tradebalance = pd.DataFrame(index=[asset], data=res['result']).T

        if not tradebalance.empty:
            tradebalance.loc[:, asset] = tradebalance[asset].astype(float)

        return tradebalance

    @crl_sleep
    @callratelimiter('other')
    def get_open_orders(self, trades=False, userref=None, otp=None):
        """
        Get open orders info.

        Return a dictionary of open orders info.

        Parameters
        ----------
        trades : bool, optional (default=False)
            Whether or not to include trades in output.

        userref : int, optional (default=None)
            Restrict results to given user reference id.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        open : pd.DataFrame
            refid = Referral order transaction id that created this order
            userref = user reference id
            status = status of order:
                pending = order pending book entry
                open = open order
                closed = closed order
                canceled = order canceled
                expired = order expired
            opentm = unix timestamp of when order was placed
            starttm = unix timestamp of order start time (or 0 if not set)
            expiretm = unix timestamp of order end time (or 0 if not set)
            descr = order description info
                pair = asset pair
                type = type of order (buy/sell)
                ordertype = order type (See Add standard order)
                price = primary price
                price2 = secondary price
                leverage = amount of leverage
                order = order description
                close = conditional close order description (if conditional
                    close set)
            vol = volume of order (base currency unless viqc set in oflags)
            vol_exec = volume executed (base currency unless viqc set in
                oflags)
            cost = total cost (quote currency unless unless viqc set in oflags)
            fee = total fee (quote currency)
            price = average price (quote currency unless viqc set in oflags)
            stopprice = stop price (quote currency, for trailing stops)
            limitprice = triggered limit price (quote currency, when limit
                based order type triggered)
            misc = comma delimited list of miscellaneous info
                stopped = triggered by stop price
                touched = triggered by touch price
                liquidated = liquidation
                partial = partial fill
            oflags = comma delimited list of order flags
                viqc = volume in quote currency
                fcib = prefer fee in base currency (default if selling)
                fciq = prefer fee in quote currency (default if buying)
                nompp = no market price protection
            trades = array of trade ids related to order (if trades info
                requested and data available)

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Unless otherwise stated, costs, fees, prices, and volumes are in the
        asset pair's scale, not the currency's scale. For example, if the asset
        pair uses a lot size that has a scale of 8, the volume will use a scale
        of 8, even if the currency it represents only has a scale of 2.
        Similarly, if the asset pair's pricing scale is 5, the scale will
        remain as 5, even if the underlying currency has a scale of 8.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('OpenOrders', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        openorders = pd.DataFrame(res['result']['open']).T

        if not openorders.empty:
            descr = openorders.descr.apply(pd.Series)
            descr.columns = ['descr_{}'.format(col) for col in descr.columns]
            del openorders['descr']
            openorders = pd.concat((openorders, descr), axis=1)
            for col in ['expiretm', 'opentm', 'starttm']:
                openorders.loc[:, col] = openorders[col].astype(int)
            for col in ['cost', 'fee', 'price', 'vol', 'vol_exec',
                        'descr_price', 'descr_price2']:
                openorders.loc[:, col] = openorders[col].astype(float)

        return openorders

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_closed_orders(self, trades=False, userref=None, start=None,
                          end=None, ofs=None, closetime=None, otp=None):
        """Get closed orders info.

        Return a ``pd.DataFrame`` of closed orders info.

        Parameters
        ----------
        trades : bool, optional (default=False)
            Whether or not to include trades in output.

        userref : int, optional (default=None)
            Restrict results to given user reference id.

        start : int, optional (default=None)
            Starting unixtime or order tx id of results (exclusive).

        end : int, optional (default=None)
            Ending unixtime or order tx id of results (inclusive)-

        ofs : ?, optional (default=None)
            Result offset.

        closetime : str, optional (default=None)
            Which time to use, must be one of {'open', 'close', 'both'}. If
            None (default), closetime='both'.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        closed : pd.DataFrame
            Array of order info.  See Get open orders.  Additional fields:
            closetm = unix timestamp of when order was closed
            reason = additional info on status (if any)

        count :
            Amount of available order info matching criteria.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Times given by order tx ids are more accurate than unix timestamps. If
        an order tx id is given for the time, the order's open time is used.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('ClosedOrders', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        closed = pd.DataFrame(res['result']['closed']).T

        # count
        count = res['result']['count']

        if not closed.empty:

            descr = closed.descr.apply(pd.Series)
            descr.columns = ['descr_{}'.format(col) for col in descr.columns]
            del closed['descr']
            closed = pd.concat((closed, descr), axis=1)
            for col in ['closetm', 'expiretm', 'opentm', 'starttm']:
                closed.loc[:, col] = closed[col].astype(int)
            for col in ['cost', 'fee', 'price', 'vol', 'vol_exec',
                        'descr_price', 'descr_price2']:
                closed.loc[:, col] = closed[col].astype(float)

        return closed, count

    @crl_sleep
    @callratelimiter('other')
    def query_orders_info(self, txid, trades=False, userref=None, otp=None):
        """Query orders info.

        Return a ``pd.DataFrame`` of orders info.

        Parameters
        ----------
        txid : str
            Comma delimited list of transaction ids to query info about
            (20 maximum).

        trades : bool, optional (default=False)
            Whether or not to include trades in output.

        userref : int, optional (default=None)
            Restrict results to given user reference id.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        orders : pd.DataFrame
            order_txid = order info.  See get_open_orders/get_closed_orders.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('QueryOrders', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        orders = pd.DataFrame(res['result']).T

        if not orders.empty:

            descr = orders.descr.apply(pd.Series)
            descr.columns = ['descr_{}'.format(col) for col in descr.columns]
            del orders['descr']
            orders = pd.concat((orders, descr), axis=1)
            for col in ['closetm', 'expiretm', 'opentm', 'starttm']:
                if col in orders:
                    orders.loc[:, col] = orders[col].astype(float)
            for col in ['cost', 'fee', 'price', 'vol', 'vol_exec',
                        'descr_price', 'descr_price2']:
                orders.loc[:, col] = orders[col].astype(float)

        return orders

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_trades_history(self, type='all', trades=False, start=None,
                           end=None, ofs=None, otp=None, ascending=False):
        """Get trades history.

        Return a ``pd.DataFrame`` of the trade history.

        Parameters
        ----------
        type : str, optional (default='all')
            Type of trade, must be one of:
                'all' (default)    : all types (default)
                'any position'     : any position (open or closed)
                'closed position'  : positions that have been closed
                'closing position' : any trade closing all or part of a
                                     position
                'no position'      : non-positional trades

        trades : bool, optional (default=False)
            Whether or not to include trades related to position in output.

        start : int, optional (default=None)
            Starting unixtime or trade tx id of results (exclusive).

        end : int, optional (default=None)
            Ending unixtime or trade tx id of results (inclusive).

        ofs : ?, optional (default=None)
            Result offset.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        trades : pd.DataFrame
            index = datetime
            txid = trade txid
            ordertxid = order responsible for execution of trade
            pair = asset pair
            time = unix timestamp of trade
            type = type of order (buy/sell)
            ordertype = order type
            price = average price order was executed at (quote currency)
            cost = total cost of order (quote currency)
            fee = total fee (quote currency)
            vol = volume (base currency)
            margin = initial margin (quote currency)
            misc = comma delimited list of miscellaneous info
                closing = trade closes all or part of a position

            If the trade opened a position, the following fields are also
            present in the trade info:

            posstatus = position status (open/closed)
            cprice = average price of closed portion of position (quote
                currency)
            ccost = total cost of closed portion of position (quote currency)
            cfee = total fee of closed portion of position (quote currency)
            cvol = total fee of closed portion of position (quote currency)
            cmargin = total margin freed in closed portion of position (quote
                currency)
            net = net profit/loss of closed portion of position (quote
                currency, quote currency scale)
            trades = list of closing trades for position (if available)

        count : int
            Amount of available trades info matching criteria.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Unless otherwise stated, costs, fees, prices, and volumes are in the
        asset pair's scale, not the currency's scale.

        Times given by trade tx ids are more accurate than unix timestamps.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('TradesHistory', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        trades = pd.DataFrame(res['result']['trades']).T

        # count
        count = res['result']['count']

        if not trades.empty:

            trades.index.name = 'txid'
            trades.reset_index(inplace=True)

            # append datetime, sort by it
            trades['dtime'] = pd.to_datetime(trades.time, unit='s')
            trades.sort_values('dtime', ascending=ascending, inplace=True)
            trades.set_index('dtime', inplace=True)

            # set dtypes
            for col in ['cost', 'fee', 'margin', 'price', 'time', 'vol']:
                trades.loc[:, col] = trades[col].astype(float)

        return trades, count

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_deposit_methods(self, asset='XBT', otp=None):
        """Get methods available for depositing a particular asset.

        Return a ``pd.DataFrame`` of deposit methods info.

        Parameters
        ----------
        asset : str (default='XBT')
            Asset for which to return deposit methods.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        depositmethods : pd.DataFrame
            Table containing trade balance info.
            method = name of deposit method
            limit = maximum net amount that can be deposited right now, or false if no limit
            fee = amount of fees that will be paid
            address-setup-fee = whether or not method has an address setup fee
            gen-address = whether new addresses can be generated for this method

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('DepositMethods', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        depositmethods = pd.DataFrame(index=[asset], data=res['result']).T

        return depositmethods

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_deposit_addresses(self, asset='XBT', method='Bitcoin', new=False, otp=None):
        """Get (or generate a new) deposit addresses for a particular asset and method.

        Return a ``pd.DataFrame`` of deposit methods info.

        Parameters
        ----------
        asset : str (default='XBT')
            Asset being deposited

        method : str (default='Bitcoin')
            Name of the deposit method

        new : boolean, optional (default=False)
            Whether or not to generate a new address

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        depositmethods : pd.DataFrame
            Table containing trade balance info.
            address = deposit Address
            expiretm = expiration time in unix timestamp, or 0 if not expiring
            new = whether or not address has ever been used

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('DepositAddresses', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        depositaddresses = pd.DataFrame(index=[asset], data=res['result']).T

        return depositaddresses

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_deposit_status(self, asset='XBT', method=None, otp=None):
        """Get information about recent deposits made.

        Return a ``pd.DataFrame`` of recent deposits.

        Parameters
        ----------
        asset : str (default='XBT')
            Asset being deposited

        method : str, optional (default=None)
            Name of the deposit method

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        depositstatus : pd.DataFrame
            Table containing recent deposit status info.
            method = name of deposit method
            aclass = asset class
            asset = asset
            refid = reference ID
            txid = method transaction ID
            info = method transaction information
            amount = amount deposited
            fee = fees paid
            time = unix timestamp when request was made
            status = status of deposit
            status-prop = addition status properties (if available)
                          "return": a return transaction initiated by Kraken
                          "onhold": deposit is on hold pending review

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('DepositStatus', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        depositstatus = pd.DataFrame(index=[asset], data=res['result']).T

        return depositstatus

    @crl_sleep
    @callratelimiter('other')
    def get_withdrawal_information(self, key, asset='XBT', amount=0.0, otp=None):
        """Retrieve fee information about potential withdrawals for a particular asset, key and amount.

        Return a ``pd.DataFrame`` of withdrawal info.

        Parameters
        ----------
        key : str
            Withdrawal key name, as set up on your account.

        asset : str (default='XBT')
            Asset being withdrawn.

        amount : float (default=0.0)
            Amount to be withdrawn.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required).

        Returns
        -------
        withdrawal_info : pd.DataFrame
            Table containing withdrawal info.
            method = name of asset
            limit = max. available for withdraw
            amount = withdrawn amount (fees already subtracted)
            fee = withdrawal fees

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items()
                if arg != 'self' and value is not None}

        # query
        res = self.api.query_private('WithdrawInfo', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        withdrawal_info = pd.DataFrame(index=[asset], data=res['result']).T

        return withdrawal_info

    @crl_sleep
    @callratelimiter('other')
    def withdraw_funds(self, key, asset='XBT', amount=0.0, otp=None):
        """Make a withdrawal request.

        Initialize a withdrawal and return the withdrawal refid.

        Parameters
        ----------
        key : str
            Withdrawal key name, as set up on your account.

        asset : str (default='XBT')
            Asset being withdrawn.

        amount : float (default=0.0)
            Amount to be withdrawn.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required).

        Returns
        -------
        withdrawal_refid : str
            refid of the withdraw request.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items()
                if arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Withdraw', data=data)

        # check for error
        if len(res["error"]) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    @crl_sleep
    @callratelimiter('other')
    def get_withdrawal_status(self, asset='XBT', method=None, otp=None):
        """Retrieve information about recently requests withdrawals.

        Return a ``pd.DataFrame`` of recent withdrawals.

        Parameters
        ----------
        asset : str (default='XBT')
            Asset being withdrawn.

        method : str (default=None)
            Name of the withdrawal method.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required).

        Returns
        -------
        withdrawalstatus : pd.DataFrame
            Table containing recent withdrawal status info.
            method = name of withdrawal method
            aclass = asset class
            asset = asset
            refid = reference ID
            txid = method transaction ID
            info = method transaction information
            amount = amount withdrawn
            fee = fees paid
            time = unix timestamp when request was made
            status = status of withdrawal
            status-prop = addition status properties (if available)
                          "cancel-pending" cancelation requested
                          "canceled" canceled
                          "cancel-denied" cancelation requested but was denied
                          "return" a return transaction initiated by Kraken; it cannot be canceled
                          "onhold" withdrawal is on hold pending review

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items()
                if arg != 'self' and value is not None}

        # query
        res = self.api.query_private('WithdrawStatus', data=data)

        # check for error
        if len(res["error"]) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        withdrawalstatus = pd.DataFrame(index=[asset], data=res['result']).T

        return withdrawalstatus

    @crl_sleep
    @callratelimiter('other')
    def cancel_withdrawal(self, asset='XBT', refid=None, otp=None):
        """Cancel a recently requested withdrawal, if it has not already been successfully processed.

        Returns whether cancellation was successful or not.

        Parameters
        ----------
        asset : str (default='XBT')
            Asset being withdrawn.

        refid : str (default=None)
            Withdrawal reference ID.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required).

        Returns
        -------
        succes : bool
            Whether cancellation was successful or not.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items()
                if arg != 'self' and value is not None}

        # query
        res = self.api.query_private('WithdrawCancel', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def query_trades_info(self, txid, trades=False, otp=None, ascending=False):
        """Query trades info.

        Return a ``pd.DataFrame`` of trades info.

        Parameters
        ----------
        txid : str
            Comma delimited list of transaction ids to query info about
            (20 maximum).

        trades : bool, optional (default=False)
            Whether or not to include trades related to position in output.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        trades : pd.DataFrame
            See get_trades_history.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('QueryTrades', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        trades = pd.DataFrame(res['result']).T

        if not trades.empty:

            trades.index.name = 'txid'
            trades.reset_index(inplace=True)

            # append datetime, sort by it
            trades['dtime'] = pd.to_datetime(trades.time, unit='s')
            trades.sort_values('dtime', ascending=ascending, inplace=True)
            trades.set_index('dtime', inplace=True)

            # set dtypes
            for col in ['cost', 'fee', 'margin', 'price', 'time', 'vol']:
                trades.loc[:, col] = trades[col].astype(float)

        return trades

    @crl_sleep
    @callratelimiter('other')
    def get_open_positions(self, txid=None, docalcs=False, otp=None):
        """Get open positins info.

        Return a ``pd.DataFrame`` of open positions info.

        Parameters
        ----------
        txid : str, optional (default=None)
            Comma delimited list of transaction ids to restrict output to.

        docalcs : bool, optional (default=False)
            Whether or not to include profit/loss calculations.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        openpositions : pd.DataFrame
            txid =
            ordertxid = order responsible for execution of trade
            pair = asset pair
            time = unix timestamp of trade
            type = type of order used to open position (buy/sell)
            ordertype = order type used to open position
            cost = opening cost of position (quote currency unless viqc set in
                oflags)
            fee = opening fee of position (quote currency)
            vol = position volume (base currency unless viqc set in oflags)
            vol_closed = position volume closed (base currency unless viqc set
                in oflags)
            margin = initial margin (quote currency)
            value = current value of remaining position (if docalcs requested.
                quote currency)
            net = unrealized profit/loss of remaining position (if docalcs
                requested.  quote currency, quote currency scale)
            misc = comma delimited list of miscellaneous info
            oflags = comma delimited list of order flags
                viqc = volume in quote currency

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Unless otherwise stated, costs, fees, prices, and volumes are in the
        asset pair's scale, not the currency's scale.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('OpenPositions', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        openpositions = res['result']

        return openpositions

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_ledgers_info(self, aclass=None, asset=None, type='all', start=None,
                         end=None, ofs=None, otp=None, ascending=False):
        """Get ledgers info.

        Return a ``pd.DataFrame`` of ledgers info.

        Parameters
        ----------
        aclass : str, optional (default=None)
            Asset class. If None (default), aclass='currency'.

        asset : str, optional (default=None)
            Comma delimited list of assets to restrict output to. If None
            (default), all for given asset class.

        type : str, optional (default='all')
            Type of ledger to retrieve, must be one of {'all', 'deposit',
            'withdrawal', 'trade', 'margin'}

        start : int, optional (default=None)
            Starting unixtime or ledger id of results (exclusive).

        end : int, optional (default=None)
            Ending unixtime or ledger id of results (inclusive)

        ofs : ?, optional (default=None)
            Result offset.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        ledger : pd.DataFrame
            ledger_id = ledger info
            refid = reference id
            time = unx timestamp of ledger
            type = type of ledger entry
            aclass = asset class
            asset = asset
            amount = transaction amount
            fee = transaction fee
            balance = resulting balance

        count : int
            Amount of available ledger info matching criteria.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        Times given by ledger ids are more accurate than unix timestamps.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Ledgers', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        ledgers = pd.DataFrame(res['result']['ledger']).T

        # count
        count = res['result']['count']

        if not ledgers.empty:

            ledgers.index.name = 'ledger_id'
            ledgers.reset_index(inplace=True)

            # append datetime, sort by it
            ledgers['dtime'] = pd.to_datetime(ledgers.time, unit='s')
            ledgers.sort_values('dtime', ascending=ascending, inplace=True)
            ledgers.set_index('dtime', inplace=True)

            # dtypes
            for col in ['amount', 'balance', 'fee']:
                ledgers.loc[:, col] = ledgers[col].astype(float)
            ledgers.loc[:, 'time'] = ledgers.time.astype(int)

        return ledgers, count

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def query_ledgers(self, id, otp=None, ascending=False):
        """Query ledgers info.

        Return a ``pd.DataFrame`` of ledgers info.

        Parameters
        ----------
        id : int
            Comma delimited list of ledger ids to query info about
            (20 maximum).

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        ascending : bool, optional (default=False)
            If set to True, the data frame will be sorted with the most recent
            date in the last position. When set to False, the most recent date
            is in the first position.

        Returns
        -------
        ledgers : pd.DataFrame
            ledger_id = ledger info.  See get_ledgers_info.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('QueryLedgers', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        ledgers = pd.DataFrame(res['result']).T

        if not ledgers.empty:

            ledgers.index.name = 'ledger_id'
            ledgers.reset_index(inplace=True)

            # append datetime, sort by it
            ledgers['dtime'] = pd.to_datetime(ledgers.time, unit='s')
            ledgers.sort_values('dtime', ascending=ascending, inplace=True)
            ledgers.set_index('dtime', inplace=True)

            # dtypes
            for col in ['amount', 'balance', 'fee']:
                ledgers.loc[:, col] = ledgers[col].astype(float)
            ledgers.loc[:, 'time'] = ledgers.time.astype(int)

        return ledgers

    @crl_sleep
    @callratelimiter('ledger/trade history')
    def get_trade_volume(self, pair=None, fee_info=True, otp=None):
        """Get trade volume.

        Return a ``pd.DataFrame`` of trade volume.

        Parameters
        ----------
        pair : str, optional (default=None)
            Comma delimited list of asset pairs to get fee info on. If None
            (default), no fee info is provided.

        fee_info : bool, optional (default=True)
            Whether or not to include fee info in results.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        currency : str
            Currency (ZUSD).

        volume : float
            Current discount volume.

        fees : pd.DataFrame
            Asset pairs and fee tier info (if requested)
                fee = current fee in percent
                minfee = minimum fee for pair (if not fixed fee)
                maxfee = maximum fee for pair (if not fixed fee)
                nextfee = next tier's fee for pair (if not fixed fee. nil if
                    at lowest fee tier)
                nextvolume = volume level of next tier (if not fixed fee. nil
                    if at lowest fee tier)
                tiervolume = volume level of current tier (if not fixed fee.
                    nil if at lowest fee tier)

        fees_maker : pd.DataFrame
            Asset pairs and maker fee tier info (if requested) for any pairs on
                    maker/taker schedule
                fee = current fee in percent
                minfee = minimum fee for pair (if not fixed fee)
                maxfee = maximum fee for pair (if not fixed fee)
                nextfee = next tier's fee for pair (if not fixed fee. nil if
                    at lowest fee tier)
                nextvolume = volume level of next tier (if not fixed fee. nil
                    if at lowest fee tier)
                tiervolume = volume level of current tier (if not fixed fee.
                    nil if at lowest fee tier)

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        Notes
        -----
        If an asset pair is on a maker/taker fee schedule, the taker side is
        given in "fees" and maker side in "fees_maker". For pairs not on
        maker/taker, they will only be given in "fees".

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('TradeVolume', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        volume = float(res['result']['volume'])

        # fees
        try:
            fees = pd.DataFrame(res['result']['fees'])
            for col in fees.columns:
                fees.loc[:, col] = fees[col].astype(float)
        except KeyError:
            fees = None
        try:
            fees_maker = pd.DataFrame(res['result']['fees_maker'])
            for col in fees_maker.columns:
                fees_maker.loc[:, col] = fees_maker[col].astype(float)
        except KeyError:
            fees_maker = None

        # currency
        currency = res['result']['currency']

        return currency, volume, fees, fees_maker

    def add_standard_order(self, ordertype, type, pair, userref=None,
                           volume=None, price=None, price2=None,
                           trigger=None, leverage=None, oflags=None,
                           timeinforce=None, starttm=0, expiretm=0,
                           close_ordertype=None, close_price=None,
                           close_price2=None, deadline=None, validate=True,
                           otp=None):
        """Place a new order.

        Parameters
        ----------
        ordertype : str
            Order type, one of:
            ["market", "limit", "stop-loss", "take-profit", "stop-loss-limit",
             "take-profit-limit", "settle-position"]

        type : str
            Order direction (buy/sell), one of:
            ["buy", "sell"]

        pair : str
            Asset pair id or altname.

        userref : int, optional (default=None)
            User reference id.
            userref is an optional user-specified integer id that can be
            associated with any number of orders. Many clients choose a userref
            corresponding to a unique integer id generated by their systems
            (e.g. a timestamp). However, because we don't enforce uniqueness on
            our side, it can also be used to easily group orders by pair, side,
            strategy, etc. This allows clients to more readily cancel or query
            information about orders in a particular group, with fewer API
            calls by using userref instead of our txid, where supported.

        volume : str, optional (default=None)
            Order quantity in terms of the base asset
            Note: Volume can be specified as 0 for closing margin orders to
                  automatically fill the requisite quantity.

        price : str, optional (default=None)
            Price.
            - Limit price for limit orders
            - Trigger price for stop-loss, stop-loss-limit, take-profit and
              take-profit-limit orders

        price2 : str, optional (default=None)
            Secondary Price. Limit price for stop-loss-limit and
            take-profit-limit orders.
            Note: Either price or price2 can be preceded by +, -, or # to
            specify the order price as an offset relative to the last traded
            price. + adds the amount to, and - subtracts the amount from the
            last traded price. # will either add or subtract the amount to the
            last traded price, depending on the direction and order type used.
            Relative prices can be suffixed with a % to signify the relative
            amount as a percentage.

        trigger : str, optional (default=None)
            Price signal used to trigger stop-loss, stop-loss-limit,
            take-profit and take-profit-limit orders. One of ["index", "last"].
            Note: This trigger type will as well be used for associated
            conditional close orders.

        leverage : str, optional (default=None)
            Amount of leverage desired.

        oflags : str, optional (default=None)
            Comma delimited list of order flags:
            - post post-only order (available when ordertype = limit)
            - fcib prefer fee in base currency (default if selling)
            - fciq prefer fee in quote currency (default if buying, mutually
              exclusive with fcib)
            - nompp disable market price protection for market orders

        timeinforce : str, optional (default=None)
            One of ["GTC", "IOC", "GTD"].
            Time-in-force of the order to specify how long it should remain in
            the order book before being cancelled. GTC (Good-'til-cancelled) is
            default if the parameter is omitted. IOC (immediate-or-cancel) will
            immediately execute the amount possible and cancel any remaining
            balance rather than resting in the book. GTD (good-'til-date), if
            specified, must coincide with a desired expiretm.

        starttm : int, optional (default=0)
            Scheduled start time. Can be specified as an absolute timestamp or
            as a number of seconds in the future.
            0 = now (default)
            +<n> = schedule start time seconds from now
            <n> = unix timestamp of start time

        expiretm : int, optional (default=0)
            Expiration time.
            0 = no expiration (default)
            +<n> = expire seconds from now, minimum 5 seconds
            <n> = unix timestamp of expiration time

        close_ordertype : str, optional (default=None)
            Conditional close order type, one of ["limit", "stop-loss",
            "take-profit", "stop-loss-limit", "take-profit-limit"].
            Note: Conditional close orders are triggered by execution of the
            primary order in the same quantity and opposite direction, but once
            triggered are independent orders that may reduce or increase net
            position.

        close_price : str, optional (default=None)
            Conditional close order price

        close_price2 : str, optional (default=None)
            Conditional close order price2

        deadline : str, optional (default=None)
            RFC3339 timestamp (e.g. 2021-04-01T00:18:45Z) after which the
            matching engine should reject the new order request, in presence of
            latency or order queueing. min now() + 2 seconds, max now() + 60
            seconds.

        validate : bool, optional (default=True)
            Validate inputs only. Do not submit order (default).

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        res : dict
            res['descr'] = order description info
                order = order description
                close = Conditional close order description, if applicable
            res['txid'] = array of transaction ids for order (if order was
                added successfully)

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.
            Errors: errors include (but are not limited to):
            EGeneral:Invalid arguments
            EService:Unavailable
            ETrade:Invalid request
            EOrder:Cannot open position
            EOrder:Cannot open opposing position
            EOrder:Margin allowance exceeded
            EOrder:Margin level too low
            EOrder:Insufficient margin (exchange does not have sufficient funds
                to allow margin trading)
            EOrder:Insufficient funds (insufficient user funds)
            EOrder:Order minimum not met (volume too low)
            EOrder:Orders limit exceeded
            EOrder:Positions limit exceeded
            EOrder:Rate limit exceeded
            EOrder:Scheduled orders limit exceeded
            EOrder:Unknown position

        Notes
        -----

        See get_tradable_asset_pairs for details on the available trading
        pairs, their price and quantity precisions, order minimums, available
        leverage, etc.

        """

        # create data dictionary
        if validate is False:
            validate = None
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # This little hack fixes the problem with [ ]
        if "close_ordertype" in data:
            data["close[ordertype]"] = data.pop("close_ordertype")

        if "close_price" in data:
            data["close[price]"] = data.pop("close_price")

        if "close_price2" in data:
            data["close[price2]"] = data.pop("close_price2")

        # query
        res = self.api.query_private('AddOrder', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    def cancel_open_order(self, txid, otp=None):
        """Cancel open order(s).

        Cancel open order with transaction id ``txid``.

        Parameters
        ----------
        txid : str
            Transaction id.

        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        count : int
            Number of orders canceled.

        pending : bool
            If set, order(s) is/are pending cancellation.

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        Notes
        -----
        txid may be a user reference id.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # submit
        res = self.api.query_private('CancelOrder', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    def datetime_to_unixtime(self, dt):
        """Return unixtime for a given datetime.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to convert to unixtime.

        Returns
        -------
        unixtime : int
            The unixtime corresponding to the given datetime.

        """

        delta_t = (dt - datetime.datetime(1970, 1, 1)).total_seconds()
        unixtime = int(delta_t)

        return unixtime

    def unixtime_to_datetime(self, unixtime):
        """Return datetime (UTC) for a given unixtime.

        Parameters
        ----------
        unixtime : int
            The unixtime to convert to datetime.

        Returns
        -------
        datetime : datetime.datetime
            The datetime (UTC) corresponding to the given unixtime.

        """

        dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(0, unixtime)

        return dt

    def _decrease_api_counter(self):

        # decrease api counter, update time of last query
        now = datetime.datetime.now()
        decr = int((now - self.time_of_last_query).seconds / self.factor)
        self.api_counter -= decr
        if self.api_counter < 0:
            self.api_counter = 0
        self.time_of_last_query = now

    @crl_sleep
    @callratelimiter('other')
    def get_stakeable_assets(self, otp=None):
        """Get list of stakeable assets and staking details.

        Return a ``pd.DataFrame`` of asset that the user is able to stake.
        This operation requires an API key with both `Withdraw funds` and
        `Query funds` permission.

        Parameters
        ----------
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        assets : pd.DataFrame
            Table containing asset names staking details.
            index = asset name
            method = Unique ID of the staking option (used in Stake/Unstake
            operations)
            staking_asset = Staking asset code/name
            on_chain = Whether the staking operation is on-chain or not.
            can_stake = Whether the user will be able to stake this asset.
            can_unstake = Whether the user will be able to unstake this asset.
            rewards.reward = Reward earned while staking.
            rewards.type = Reward type.
            minimum_amount.staking = minimum amount that can be staked.
            minimum_amount.unstaking = minimum amount that can be unstaked

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Staking/Assets', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        assets = pd.json_normalize(data=res['result']).set_index('asset')

        return assets

    @crl_sleep
    @callratelimiter('other')
    def get_pending_staking_transactions(self, otp=None):
        """Get list of pending staking transactions.

        Returns a ``pd.DataFrame`` of pending staking transactions.

        Parameters
        ----------
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        transactions : pd.DataFrame
            Table containing transaction refids and details.
            index = refid
            type = Type of transaction {'bonding', 'reward', 'unbonding'}
            asset = Asset code/name
            amount = The transaction amount
            time = Unix timestamp when the transaction was initiated.
            bond_start = Unix timestamp from the start of bond period
            (applicable only to `bonding` transactions).
            bond_end = Unix timestamp of the end of bond period
            (applicable only to `bonding` transactions).
            status = Transaction status {'Initial', 'Pending', 'Settled'
            'Success', 'Failure'}

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Staking/Pending', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        try:
            transactions = pd.json_normalize(
                data=res['result']
            ).set_index('refid')
        except KeyError:
            return None

        return transactions

    @crl_sleep
    @callratelimiter('other')
    def get_staking_transactions(self, otp=None):
        """Returns the list of 1000 recent staking transactions from past
        90 days.

        Returns a ``pd.DataFrame`` of staking transactions.

        Parameters
        ----------
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        transactions : pd.DataFrame
            Table containing transaction refids and details.
            index = refid
            type = Type of transaction {'bonding', 'reward', 'unbonding'}
            asset = Asset code/name
            amount = The transaction amount
            time = Unix timestamp when the transaction was initiated.
            bond_start = Unix timestamp from the start of bond period
            (applicable only to `bonding` transactions).
            bond_end = Unix timestamp of the end of bond period
            (applicable only to `bonding` transactions).
            status = Transaction status {'Initial', 'Pending', 'Settled'
            'Success', 'Failure'}

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Staking/Transactions', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        # create dataframe
        try:
            transactions = pd.json_normalize(
                data=res['result']
            ).set_index('refid')
        except KeyError:
            return None

        return transactions

    @crl_sleep
    @callratelimiter('other')
    def stake_asset(self, asset, amount, method, otp=None):
        """Stake an asset from your spot wallet. This operation requires an
        API key with `Withdraw funds` permission.

        Returns a ``str`` of the transaction Reference ID.

        Parameters
        ----------
        asset : str
            Asset to stake (asset ID or `altname`)
        amount : float
            Amount of the asset to stake
        method : str
            Name of the staking option to use (refer to the Staking Assets
            endpoint for the correct method names for each asset)
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        refid : str
            Transaction Reference ID

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Stake', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    @crl_sleep
    @callratelimiter('other')
    def unstake_asset(self, asset, amount, otp=None):
        """Unstake an asset from your staking wallet. This operation requires
        an API key with `Withdraw funds` permission.

        Returns a ``str`` of the transaction Reference ID.

        Parameters
        ----------
        asset : str
            Asset to unstake (asset ID or `altname`). Must be a valid staking
            asset (e.g. XBT.M, XTZ.S, ADA.S)
        amount : float
            Amount of the asset to stake
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        refid : str
            Transaction Reference ID

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """

        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('Unstake', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']

    @crl_sleep
    @callratelimiter('other')
    def get_websockets_token(self, opt=None):
        """An authentication token must be requested via this REST API endpoint
        in order to connect to and authenticate with our Websockets API. The
        token should be used within 15 minutes of creation, but it does not
        expire once a successful Websockets connection and private subscription
        has been made and is maintained.

        The 'Access WebSockets API' permission must be enabled for the API key
        in order to generate the authentication token.

        Returns a ``dict`` of the websockets token and expriry time (secs).

        Parameters
        ----------
        otp : str
            Two-factor password (if two-factor enabled, otherwise not required)

        Returns
        -------
        token: str
            Websockets token
        expires : int
            Time (in seconds) after which the token expires

        Raises
        ------
        HTTPError
            An HTTP error occurred.

        KrakenAPIError
            A kraken.com API error occurred.

        CallRateLimitError
            The call rate limiter blocked the query.

        """
        # create data dictionary
        data = {arg: value for arg, value in locals().items() if
                arg != 'self' and value is not None}

        # query
        res = self.api.query_private('GetWebSocketsToken', data=data)

        # check for error
        if len(res['error']) > 0:
            raise KrakenAPIError(res['error'])

        return res['result']
