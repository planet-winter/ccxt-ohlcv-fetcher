# ccxt-ohlcv-fetcher

fetches OHLC values from most crypto exchanges using ccxt library. saves candles to a sqlite database per symbol.
by default resumes from last candle fetched.


## setup

install virtualenv and python with your OS method

```
git clone https://github.com/planet-winter/ccxt-ohlcv-fetcher
cd ccxt-ohlcv-fetcher

virtualenv --python=python3.4 virtualenv
source virtualenv/bin/activate

pip install -r requirements.txt
```

## run

display help
```
./ccxt-ohlcv-fetch.py
```
get 1 min candles of XRP/USD data from bitfinex
```
./ccxt-ohlcv-fetch.py -s 'XRP/USD' -e bitfinex -t 1m --debug  
```

## convert to CSV
```
sqlite3 bitfinex_XRPUSD_1m.sqlite

sqlite> .headers on
sqlite> .mode csv
sqlite> .output data.csv
sqlite> SELECT * FROM candles;
sqlite> .quit
```

