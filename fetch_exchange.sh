#!/usr/bin/env bash

if [ ! -z $1 ]
then
  exchange=$1
else
  echo "please provide an exchange as argument"
  exit 1
fi

source virtualenv/bin/activate

echo "$(./ccxt-ohlcv-fetch.py -e $exchange -s XXX -t 1m | grep '  -' | tr -d "  -")" > ${exchange}_all_symbols.txt
split -n l/4 ${exchange}_all_symbols.txt ${exchange}_symbols.

echo "using exchange $exchange fetching pairs:"

for symbols in  ${exchange}_symbols.*; do
    cat $symbols | ( while read symbol; do
	echo "Starting process for $symbol"
	./ccxt-ohlcv-fetch.py -e bitfinex -s "$symbol" -t 1m --debug -q
    done ) &
done

