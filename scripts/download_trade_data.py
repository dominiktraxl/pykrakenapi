"""
Download trade data for a kraken asset pair. Updates can be downloaded by
simply calling this script again.

Data is stored as pandas.DataFrame's (in "unixtimestamp.pickle" format).
Use pd.read_pickle(file) to load data into memory.

Use the ``interval`` argument to sample trade data into ohlc format instead of
downloading/updating trade data (in that case, only the arguments ``folder``,
``pair`` and ``interval`` have an effect). Data is stored as a pandas.DataFrame
(in "pair_interval.pickle" format).

"""

import argparse
import os
from pathlib import Path
import time
import pytz

import pandas as pd
import krakenex
from pykrakenapi import KrakenAPI

from pykrakenapi.pykrakenapi import CallRateLimitError

# parser
parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument(
    '--folder',
    help='which (parent) folder to store data in',
    type=str,
    default=str(Path.home()) + '/cryptodata/')

parser.add_argument(
    '--pair',
    help=('asset pair to get trade data for. '
          'see KrakenAPI(api).get_tradable_asset_pairs().index.values'),
    type=str,
    default='XXBTZEUR')

parser.add_argument(
    '--since',
    help=("return trade data since given unixtime (exclusive). If 0 (default) "
          "and this script was called before, only an update to the "
          "most recent data is retrieved. If 0 and this function was not "
          "called before, retrieve from earliest time possible."),
    type=str,
    default=0)

parser.add_argument(
    '--timezone',
    help=("convert the timezone of timestamps to ``timezone``, which must be "
          "a string that pytz.timezone() accepts (see pytz.all_timezones)"),
    type=str,
    default='Europe/Berlin')

parser.add_argument(
    '--interval',
    help=('sample downloaded trade data to ohlc format with the given time '
          'interval (minutes). If 0 (default), only download/update trade '
          'data.'),
    type=int,
    default=0)

parser.add_argument(
    '--retry',
    help='retry query after ``retry`` seconds whenever an '
         'HTTPError/KrakenAPIError occurs',
    type=int,
    default=1)

parser.add_argument(
    '--tier',
    help='your kraken tier level',
    type=int,
    default=3)

parser.add_argument(
    '--sleep',
    help='sleep for ``sleep`` seconds whenever the '
         'call rate limit was exceeded.',
    type=int,
    default=4)

# args
args = parser.parse_args()

folder = args.folder
pair = args.pair
since = args.since
timezone = args.timezone
retry = args.retry
tier = args.tier
sleep = args.sleep
interval = args.interval


class GetTradeData(object):

    def __init__(self, folder, pair, retry, tier, sleep, timezone):

        # initiate api
        self.api = krakenex.API()
        self.k = KrakenAPI(self.api, tier, retry)
        self.sleep = sleep

        # set pair
        self.pair = pair
        self.tz = pytz.timezone(timezone)

        # set and create folder
        self.folder = folder
        os.makedirs(self.folder + pair, exist_ok=True)

    def download_trade_data(self, since):

        folder = self.folder + self.pair + '/'

        # update or new download?
        if since is 0:
            fs = os.listdir(folder)
            if len(fs) > 0:
                fs.sort()
                last = int(fs[-1].split('.')[0])
            else:
                last = 0
        else:
            last = since

        # get data
        attempt = 0
        while True:
            try:
                fname = folder + '{}.pickle'.format(str(last).zfill(19))
                trades, last = self.k.get_recent_trades(pair=self.pair,
                                                        since=last)

                # set timezone
                index = trades.index.tz_localize(pytz.utc).tz_convert(self.tz)
                trades.index = index

                # store
                print('storing', fname)
                trades.to_pickle(fname)

                # reset attempt
                attempt = 0

            except CallRateLimitError as err:
                print('CallRateLimitError: {} |'.format(str(attempt).zfill(3)),
                      err)
                attempt += 1
                time.sleep(self.sleep)
                continue

            except ValueError:
                print('download/update finished!')
                break

    def agg_ohlc(self, interval):

        folder = self.folder + self.pair + '/'

        # fetch files and convert to dataframe
        fs = os.listdir(folder)
        fs.sort(reverse=True)
        trades = []
        for f in fs:
            trades.append(pd.read_pickle(folder + f))
        trades = pd.concat(trades, axis=0)
        trades.loc[:, 'cost'] = trades.price * trades.volume

        # resample
        gtrades = trades.resample('{}min'.format(interval))

        # ohlc, volume
        ohlc = gtrades.price.ohlc()
        ohlc.loc[:, 'vol'] = gtrades.volume.sum()
        ohlc.vol.fillna(0, inplace=True)
        closes = ohlc.close.fillna(method='pad')
        ohlc = ohlc.apply(lambda x: x.fillna(closes))

        # vwap
        ohlc.loc[:, 'vwap'] = gtrades.cost.sum() / ohlc.vol
        ohlc.vwap.fillna(ohlc.close, inplace=True)

        # count
        ohlc.loc[:, 'count'] = gtrades.size()

        # store on disc
        fname = self.folder + self.pair + '_{}.pickle'.format(interval)
        print('storing', fname)
        ohlc.to_pickle(fname)


dl = GetTradeData(folder, pair, retry, tier, sleep, timezone)

if interval == 0:
    dl.download_trade_data(since)
else:
    dl.agg_ohlc(interval)
