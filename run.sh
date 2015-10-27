#!/bin/bash

INPUT=$1
FILESIZE=$(wc -l $1 | cut -d ' ' -f 4)
time cat $1 | python cluster.py | pv -p -e -l -s $FILESIZE
