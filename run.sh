#!/bin/bash

INPUT=$1
WC=$(wc -l $1)
FILESIZE=$(echo $WC | cut -d ' ' -f 1)
time tail +2 $1 | python cluster.py $2 $3 $4 | pv -a -p -e -l -s $FILESIZE
