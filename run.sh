#!/bin/bash

INPUT=$1
WC=$(wc -l $1)
FILESIZE=$(echo $WC | cut -d ' ' -f 1)
time cat $1 | python augment.py $2 $3 $4 | pv -a -p -e -l -s $FILESIZE | \
  psql -d census -c 'COPY augmented FROM stdin WITH CSV'

# example (if you have created a `temp` table with appropriate columns):
# ./run.sh ../data/public.csv 5 6 censustracts
