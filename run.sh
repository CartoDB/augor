#!/bin/bash

INPUT=$1
WC=$(wc -l $1)
FILESIZE=$(echo $WC | cut -d ' ' -f 1)
time cat $1 | python augment.py $2 | pv -a -r -p -e -l -s $FILESIZE | psql -d census

# example
# ./run.sh ../data/taxi.csv ../../crackedtiles/pgsample/load/taxis.json
