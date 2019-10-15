#!/usr/bin/env bash

db=$1
if [ -z $db ]; then
  echo "usage:  $0 database.sqlite"
  exit 1
fi

csv=$(basename $db | sed 's/sqlite/csv/')

cat | sqlite3 $db << EOF
.headers on
.mode csv
.output $csv
SELECT * FROM candles;
.quit
EOF

exit 0
