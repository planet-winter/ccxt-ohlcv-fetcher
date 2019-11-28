#!/usr/bin/env python3


__author__ = 'Daniel Winter'

import ccxt
import time
import math
import argparse
import signal
import sys
import os
import re
import sqlite3
#import ccxt.async_support as ccxt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Index
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError


DEFAULT_SINCE = "2014-01-01T00:00:00Z"
DEFAULT_SLEEP_SECONDS = 5*60
DEFAULT_RETRIES = 5
DEFAULT_MIN_BATCH_LEN = 24 * 60
EXTRA_RATE_LIMIT = 0

Base = declarative_base()


class Candle(Base):
    __tablename__ = 'candles'

    timestamp = Column(Integer, primary_key=True)
    open = Column(String)
    high = Column(String)
    low = Column(String)
    close = Column(String)
    volume = Column(String)

    Index('timestamp_idx', 'timestamp')

    def __repr__(self):
        return "<Candle(timestamp='%s', open='%s', high='%s', low='%s', \
                        close='%s', volume='%s')>" % (self.timestamp, \
                        self.open, self.high, self.low, \
                        self.close, self.volume)



def perist_ohlcv_batch(session, ohlcv_batch, exchange, debug=False):
    for ohlcv in ohlcv_batch:
        candle = Candle(
            timestamp=int(ohlcv[0]),
            open=ohlcv[1],
            high=ohlcv[2],
            low=ohlcv[3],
            close=ohlcv[4],
            volume=ohlcv[5])
        try:
            session.add(candle)
        except IntegrityError:
            try:
                session.rollback()
            except:
                quit()

    session.commit()
    if debug:
        for candle in ohlcv_batch:
            print(exchange.iso8601(candle[0]), candle)

def get_last_candle_timestamp(session):
    last_timestamp = session.query(Candle).order_by(desc(Candle.timestamp)).limit(1).all()
    if last_timestamp != []:
        return int(last_timestamp[0].timestamp)
    else:
        return None


def get_ohlcv_batch(exchange, symbol, timeframe, since, session, debug=False):
    ohlcv_batch = []
    try:
        time.sleep(EXTRA_RATE_LIMIT)
        ohlcv_batch = exchange.fetch_ohlcv(symbol, timeframe, since)
    except:
        # TODO: handle specific exeptions
        time.sleep(DEFAULT_SLEEP_SECONDS)

    if ohlcv_batch is not None and len(ohlcv_batch):
        # ohlcv_batch[0] contains candle at time "since"
        # which we already fetched in last call
        ohlcv_batch = ohlcv_batch[1:]
        return ohlcv_batch
    else:
        return None


def get_candles(exchange, session, symbol, timeframe, since, doquit, debug):
    exchange_milliseconds = exchange.milliseconds()

    while True:

        ohlcv_batch = get_ohlcv_batch(exchange, symbol, timeframe,
                           since, session, debug)

        if ohlcv_batch is not None and len(ohlcv_batch):
            last_candle = ohlcv_batch[-1]
            last_candle_timestamp = since = last_candle[0]

            if last_candle_is_incomplete(last_candle_timestamp, timeframe, exchange):
                # delete last incomplete candle from list
                del ohlcv_batch[-1]
                perist_ohlcv_batch(session, ohlcv_batch, exchange, debug)
                # data is up to date with current time as well
                if doquit:
                    quit()
                else:
                    time.sleep(TIMEFRAMES[timeframe])
            else:
                perist_ohlcv_batch(session, ohlcv_batch, exchange, debug)



def gen_db_name(exchange, symbol, timeframe):
    symbol_out = symbol.replace("/", "")
    file_name = '{}_{}_{}.sqlite'.format(exchange, symbol_out, timeframe)
    full_path = os.path.join('ccxt', exchange, symbol_out, timeframe, file_name)
    return full_path

def last_candle_is_incomplete(candle_timestamp, candle_timeframe, exchange):
    timeframe_re = re.compile(r'(?P<number>\d+)(?P<unit>[smhdwMy]{1})')
    match = timeframe_re.match(candle_timeframe)
    seconds = minutes = hours = days = weeks = months = years = 0
    lookup_dict = {'s': seconds, 'm': minutes, 'h': hours, 'd': days, 'w': weeks,
        'M': months, 'y': years}\

    if match is not None:
        matchdict = match.groupdict()
        lookup_dict[matchdict['unit']] = int(matchdict['number'])
        candle_dt = datetime.fromtimestamp(candle_timestamp / 1000)
        exchange_dt = datetime.fromtimestamp(exchange.milliseconds() / 1000)
        # eg. timeframe=1d and candle_timestamp=2019-01-01T00:00:00Z
        #  exchange_dt=2019-01-02T01:00:00Z
        #
        #  2019-01-02T01:00:00Z - 1 day = 2019-01-01T00:00:00Z
        # use relativetimedelta as included batteries don't offer years
        #  or months
        one_candle_delta = relativedelta(years=lookup_dict['y'],
            months=lookup_dict['M'], weeks=lookup_dict['w'],
            days=lookup_dict['d'], hours=lookup_dict['h'],
            minutes=lookup_dict['m'], seconds=lookup_dict['s'])
        return exchange_dt - one_candle_delta < candle_dt

    else:
        message("Could not parse timeframe %s" % candle_timeframe, header="Error")

def message(message, header="Error"):
    print(header.center(80, '-'))
    print(message)
    print('-'*80)

def parse_args():
    parser = argparse.ArgumentParser(description='CCXT Market Data Downloader')

    parser.add_argument('-s', '--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair To Download')

    parser.add_argument('-e', '--exchange',
                        type=str,
                        required=True,
                        help='The exchange to download from')

    parser.add_argument('-t', '--timeframe',
                        type=str,
                        help='The timeframe to download. examples: 1m, 5m, \
                                15m, 30m, 1h, 2h, 3h, 4h, 6h, 12h, 1d, 1M, 1y')

    parser.add_argument('--since',
                        type=str,
                        help='The iso 8601 starting fetch date. Eg. 2018-01-01T00:00:00Z')

    parser.add_argument('--debug',
                        action = 'store_true',
                        help=('Print Sizer Debugs'))

    parser.add_argument('-r', '--rate-limit',
                        type=int,
                        help='eg. 20 to increase the default exchange rate limit by 20 percent')

    parser.add_argument('-q', '--quit',
                        action = 'store_true',
                        help='exit program after fetching latest candle')


    return parser.parse_args()


def check_args(args):
    # Get our Exchange

    params = {}
    try:
        params['exchange'] = getattr(ccxt, args.exchange)({
           'enableRateLimit': True,
        })
    except AttributeError:
        message('Exchange "{}" not found. Please check the exchange\
                is supported.'.format(args.exchange), header='Error')
        quit()

    if args.rate_limit:
        params['exchange'].rateLimit = int(params['exchange'].rateLimit \
                                       * (1 + args.rate_limit/100))

    # Check if fetching of OHLC Data is supported
    if params['exchange'].has["fetchOHLCV"] == False:
        message('{} does not support fetching OHLCV data. Please use another\
                exchange'.format(args.exchange), header='Error')
        quit()

    if params['exchange'].has['fetchOHLCV'] == 'emulated':
        message('{} uses emulated OHLCV. This script does not support\
                this'.format(args.exchange), header='Error')
        quit()

    # Check requested timeframe is available. If not return a helpful error.
    if args.timeframe not in params['exchange'].timeframes:
        message('The requested timeframe ({}) is not available from {}\n\
                Available timeframes are:\n{}'.format(args.timeframe, \
                args.exchange, ''.join(['  -' + key + '\n' for key in \
                params['exchange'].timeframes.keys()])), header='Error')
        quit()
    else:
        params['timeframe'] = args.timeframe

    # Check if the symbol is available on the Exchange
    params['exchange'].load_markets()
    if args.symbol not in params['exchange'].symbols:
        message('The requested symbol ({}) is not available from {}\n\
                Available symbols are:\n{}'.format(args.symbol,args.exchange, \
                ''.join(['  -' + key + '\n' \
                for key in params['exchange'].symbols])), \
                header='Error')
        quit()
    else:
        params['symbol'] = args.symbol


    db_path = gen_db_name(args.exchange, args.symbol, args.timeframe)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db_connection = 'sqlite:///' + db_path
    engine = create_engine(db_connection)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)

    params['sqlsession'] = Session()

    since = None
    if not args.since:
        params['since'] = get_last_candle_timestamp(params['sqlsession'])
        if params['since'] is None:
            params['since'] = params['exchange'].parse8601(DEFAULT_SINCE)
            message('Starting with default since value of \
                    {}.'.format(DEFAULT_SINCE), header='Info')

        else:
            if args.debug:
                message('resuming from last db entry \
                        {}'.format(params['exchange'].iso8601(params['since']))\
                        , header='Info')
    else:
        params['since'] = params['exchange'].parse8601(args.since)
        if params['since'] is None:
            message('Could not parse --since. Use format 2018-12-24T00:00:00Z',
                    header='Error')
            quit()

    if not params['exchange'].has['fetchOHLCV']:
        message('Exchange "{}" has no method fetchOHLCV.'.format(\
                args.exchange), header='Error')
        quit()

    params['debug'] = args.debug
    params['doquit'] = args.quit

    return params


def main():
    args = parse_args()
    params = check_args(args)
    p = params
    get_candles(p['exchange'], p['sqlsession'], p['symbol'], p['timeframe'], \
                p['since'], p['doquit'], p['debug'])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        message("Fetcher finished by user", header='ERROR')
    except Exception as err:
        message("Fetcher failed with exception\n {}".format(err), \
                header='ERROR')
        raise
