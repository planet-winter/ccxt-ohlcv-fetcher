#!/usr/bin/env python3


__author__ = 'Daniel Winter'

import ccxt
#import ccxt.async_support as ccxt
from datetime import datetime, timedelta
import time
import math
import argparse
import signal
import sys
import os
import sqlite3
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
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600,
              '2h': 7200, '3h': 10800, '4h': 14400, '6h': 21600 , '12h': 43200,
              '1d': 86400, '1M': 2592000, '1y': 31104000}
              # days, month and years length vary as of calendar
              # use lower value here to be safe

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
        #if debug:
        #    print(exchange.iso8601(candle.timestamp), candle)
    session.commit()


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

    if len(ohlcv_batch):
        # ohlcv_batch[0] contains candle at time "since"
        # which we already fetched in last call
        ohlcv_batch = ohlcv_batch[1:]
        if debug:
            for candle in ohlcv_batch:
                print(exchange.iso8601(candle[0]), candle)
        return ohlcv_batch
    else:
        return None


def get_candles(exchange, symbol, timeframe, since, session, doquit, debug):
    exchange_milliseconds = exchange.milliseconds()

    while True:
        # if the difference is less than 1 minute,
        # it means it is up to date with live data
        if timeframes_difference(timeframe, exchange_milliseconds, since) < 1:
            # update exchange milliseconds as fetching historical
            # usually uses more than one minute as of rate rateLimit.
            # saving some api calls
            exchange_milliseconds = exchange.milliseconds()
            if timeframes_difference(timeframe, exchange_milliseconds, since) < 1:
                # we are current
                if doquit:
                    quit()
                else:
                    time.sleep(TIMEFRAMES[timeframe])

        ohlcv_batch = get_ohlcv_batch(exchange, symbol, timeframe,
                           since, session, debug)

        if len(ohlcv_batch):
            # last candles timestamp
            since = ohlcv_batch[-1][0]
            perist_ohlcv_batch(session, ohlcv_batch, exchange, debug)


def gen_db_name(exchange, symbol, timeframe):
    symbol_out = symbol.replace("/", "")
    file_name = '{}_{}_{}.sqlite'.format(exchange, symbol_out, timeframe)
    full_path = os.path.join('ccxt', exchange, symbol_out, timeframe, file_name)
    return full_path


def timeframes_difference(timeframe, exchange_milliseconds, since):
    milliseconds_difference = exchange_milliseconds - since
    minutes_difference = milliseconds_difference / 1000 / 60
    timeframes_difference = minutes_difference / TIMEFRAMES[timeframe]
    return timeframes_difference

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


def main():
    def signal_handler(signal, frame):
        session.close()
        message('Program interrupted', header='Error')
        sys.exit(1)

    # Get our arguments
    args = parse_args()

    # Get our Exchange
    try:
        exchange = getattr(ccxt, args.exchange)({
           'enableRateLimit': True,
        })
    except AttributeError:
        message('Exchange "{}" not found. Please check the exchange is \
                supported.'.format(args.exchange), header='Error')
        quit()

    if args.rate_limit:
        EXTRA_RATE_LIMIT = int(exchange.rateLimit * (1 + args.rate_limit/100))

    # Check if fetching of OHLC Data is supported
    if exchange.has["fetchOHLCV"] == False:
        message('{} does not support fetching OHLCV data. Please use another \
                exchange'.format(args.exchange), header='Error')
        quit()

    if exchange.has['fetchOHLCV'] == 'emulated':
        message('{} uses emulated OHLCV. This script does not support \
                this'.format(args.exchange), header='Error')
        quit()

    # Check requested timeframe is available. If not return a helpful error.
    if args.timeframe not in exchange.timeframes:
        message('The requested timeframe ({}) is not available from {}\n \
                Available timeframes are:\n{}'.format(args.timeframe, \
                args.exchange, ''.join(['  -' + key + '\n' for key in \
                exchange.timeframes.keys()])), header='Error')
        quit()
    else:
        timeframe = args.timeframe

    # Check if the symbol is available on the Exchange
    exchange.load_markets()
    if args.symbol not in exchange.symbols:
        message('The requested symbol ({}) is not available from {}\n \
                Available symbols are:\n{}'.format(args.symbol,args.exchange, \
                ''.join(['  -' + key + '\n' for key in exchange.symbols])), \
                header='Error')
        quit()
    else:
        symbol = args.symbol


    db_path = gen_db_name(args.exchange, args.symbol, args.timeframe)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db_connection = 'sqlite:///' + db_path
    engine = create_engine(db_connection)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)

    session = Session()

    since = None
    if not args.since:
        since = get_last_candle_timestamp(session)
        if since == None:
            since = exchange.parse8601(DEFAULT_SINCE)
            message('Starting with default since value of \
                    {}.'.format(DEFAULT_SINCE), header='Info')

        else:
            if args.debug:
                message('resuming from last db entry \
                        {}'.format(exchange.iso8601(since)), header='Info')
    else:
        since = exchange.parse8601(args.since)
        if since == None:
            message('Could not parse --since. Use format 2018-12-24T00:00:00Z',
                    header='Error')
            quit()

    signal.signal(signal.SIGINT, signal_handler)

    if not exchange.has['fetchOHLCV']:
        message('Exchange "{}" has no method fetchOHLCV.'.format(\
                args.exchange), header='Error')
        quit()

    debug = args.debug
    doquit = args.quit

    get_candles(exchange, symbol, timeframe, since, session, doquit, debug)


if __name__ == "__main__":
     main()
