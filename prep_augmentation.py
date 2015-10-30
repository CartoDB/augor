#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Prep an augmentation from a CSV with headers and a WKT column `geom`
'''

import sys
import cPickle
from shapely import speedups, wkt
import rtree
import os
import redis
import csv
import logging
import ujson as json

assert speedups.available == True
speedups.enable()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

csv.field_size_limit(sys.maxsize)


class FastRtree(rtree.Rtree):
    def dumps(self, obj):
        return cPickle.dumps(obj, -1)


def populate_redis(r, aug_name, row):
    geoid = row['geoid']
    r.set('/'.join([aug_name, geoid]), json.dumps(row))


def generate_rtree(idx, row):
    geom = wkt.loads(row['geom'])
    geoid = row['geoid']
    idx.insert(int(geoid), geom.bounds)


def main(csv_path):
    dirpath = os.path.split(csv_path)[0]
    aug_name = '.'.join(os.path.split(csv_path)[1].split('.')[0:-1])
    rtree_path = os.path.join(dirpath, aug_name) + '.rtree'

    red = redis.Redis()
    red.flushdb()

    for fname in (aug_name + '.dat', aug_name +'.idx', ):
        try:
            os.remove(fname)
        except OSError:
            pass

    idx = FastRtree(rtree_path)

    with open(csv_path) as csv_file:
        for i, row in enumerate(csv.DictReader(csv_file)):
            populate_redis(red, aug_name, row)
            generate_rtree(idx, row)
            if i % 1000 == 0:
                print i


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        LOGGER.error('USAGE: python prep_augmentation.py <path/to/augmentation.csv>')
