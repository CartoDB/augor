#!/bin/bash

while read line
do
  url=$(echo "$line" | cut -d ' ' -f 1)
  metadata=$(echo "$line" | cut -d ' ' -f 2)

  curl -s "$url" | python ../augor/augment.py $metadata
done
