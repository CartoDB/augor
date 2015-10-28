#!/bin/bash

while read line
do
  url=$(echo "$line" | cut -f 1)
  name=$(echo "$line" | cut -f 2)
  lat=$(echo "$line" | cut -f 3)
  lon=$(echo "$line" | cut -f 4)
  augments=$(echo "$line" | cut -f 5)

  curl -s "$url" | tail +2 | python ../augor/augment.py $lat $lon $augments > $name.csv 2>$name.log &
done
