#!/usr/bin/env python3


__author__ = 'Daniel Winter'

import ccxt
#import ccxt.async_support as ccxt
from datetime import datetime, timedelta
import time
import math
import argparse
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError


SINCE_DEFAULT = "2017-00-00"


Base = declarative_base()


class Candle(Base):
     __tablename__ = 'candles'

     timestamp = Column(Integer, primary_key=True)
     open = Column(String)
     high = Column(String)
     low = Column(String)
     close = Column(String)
     volume = Column(String)

     def __repr__(self):
        return "<Candle(timestamp='%s', open='%s', high='%s', low='%s', close='%s', volume='%s')>" % (
                             self.timestamp, self.open, self.high, self.low, self.close, self.volume)


   
def perist_ohlcv_batch(session, ohlcv_batch, debug=False):
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
              if debug:
                  print(candle)
          except ReferenceError:
              continue
          try:
              session.commit()	
          except:
              continue

  

def get_last_candle_timestamp(session):
     last_timestamp = session.query(Candle).order_by(desc(Candle.timestamp)).limit(1).all()
     if last_timestamp != []:
          return int(last_timestamp[0].timestamp)
     else:
          return None


def get_ohlcv(exchange, symbol, timeframe, since, session, debug=False):
     if exchange.has['fetchOHLCV']:
          while since < exchange.milliseconds():
               try:
                    ohlcv_batch = exchange.fetch_ohlcv(symbol, timeframe, since)
                    time.sleep (exchange.rateLimit / 1000) # time.sleep wants seconds
               except:
                    time.sleep(5*60)
                    
               if len(ohlcv_batch):
                    since = ohlcv_batch[len(ohlcv_batch) - 1][0]
                    perist_ohlcv_batch(session, ohlcv_batch[1:], debug=debug)
     else:
          print('-'*36,' ERROR ','-'*35)
          print('Exchange "{}" has no method fetchOHLCV.'.format(args.exchange))
          print('-'*80)
          quit()
        

	
def gen_db_name(exchange, symbol, timeframe):
    symbol_out = symbol.replace("/","")
    filename = '{}_{}_{}.sqlite'.format(exchange, symbol_out, timeframe)
    return filename



def parse_args():
    parser = argparse.ArgumentParser(description='CCXT Market Data Downloader')

    parser.add_argument('-s','--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair To Download')

    parser.add_argument('-e','--exchange',
                        type=str,
                        required=True,
                        help='The exchange to download from')

    parser.add_argument('-t','--timeframe',
                        type=str,
                        default='1d',
                        choices=['1m', '5m','15m', '30m','1h', '2h', '3h', '4h', '6h', '12h', '1d', '1M', '1y'],
                        help='The timeframe to download')

    parser.add_argument('--since',
			type=str,
			help='The iso 8601 starting fetch date. Eg. 2018-01-01T00:00:00Z')

    parser.add_argument('--debug',
                        action ='store_true',
                        help=('Print Sizer Debugs'))

    return parser.parse_args()




def main():

     # Get our arguments
     args = parse_args()
     
     # Get our Exchange
     try:
          exchange = getattr(ccxt, args.exchange)({
               'enableRateLimit': True, 
          })
     except AttributeError:
          print('-'*36,' ERROR ','-'*35)
          print('Exchange "{}" not found. Please check the exchange is supported.'.format(args.exchange))
          print('-'*80)
          quit()

     # Check if fetching of OHLC Data is supported
     if exchange.has["fetchOHLCV"] == False:
          print('-'*36,' ERROR ','-'*35)
          print('{} does not support fetching OHLC data. Please use another exchange'.format(args.exchange))
          print('-'*80)
          quit()

     # Check requested timeframe is available. If not return a helpful error.
     if args.timeframe not in exchange.timeframes:
          print('-'*36,' ERROR ','-'*35)
          print('The requested timeframe ({}) is not available from {}\n'.format(args.timeframe,args.exchange))
          print('Available timeframes are:')
          for key in exchange.timeframes.keys():
               print('  - ' + key)
          print('-'*80)
          quit()

     # Check if the symbol is available on the Exchange
     exchange.load_markets()
     if args.symbol not in exchange.symbols:
          print('-'*36,' ERROR ','-'*35)
          print('The requested symbol ({}) is not available from {}\n'.format(args.symbol,args.exchange))
          print('Available symbols are:')
          for key in exchange.symbols:
               print('  - ' + key)
          print('-'*80)
          quit()


     db_name = gen_db_name(args.exchange, args.symbol, args.timeframe)
     db_connection = 'sqlite:///' + db_name
     engine = create_engine(db_connection)
     Base.metadata.create_all(engine)
     Session = sessionmaker()
     Session.configure(bind=engine)
     
     session = Session()

     if not args.since:
          since = get_last_candle_timestamp(session)
          if since == None:
               print('-'*36,' ERROR ','-'*35)
               print('Please specify a --since value.')
               print('-'*80)
               quit()
          else:
               if args.debug:
                    print('resuming from last db entry {}'.format(since))
     else:
          since = exchange.parse8601(args.since)
          if since == None:
               print('-'*36,' ERROR ','-'*35)
               print('Could not parse --since')
               print('-'*80)
               quit()
               
     get_ohlcv(exchange, args.symbol, args.timeframe, since, session, debug=args.debug)
  

     
if __name__ == "__main__":
     main()
