#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Convert geojson of census tracts into a dict that can be quickly searched
'''

import ujson as json
import logging
import time
import sys
import cPickle
from shapely.geometry import asShape
from shapely import speedups, wkt
import rtree
import os
#from rtree import index
import redis

assert speedups.available == True
speedups.enable()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


class FastRtree(rtree.Rtree):
    def dumps(self, obj):
        return cPickle.dumps(obj, -1)


def load_json(fname):
    start = time.time()
    LOGGER.info('loading JSON')
    with open(fname) as f_handle:
        data = json.load(f_handle)
    LOGGER.info('loaded JSON [%s]', time.time() - start)
    return data


def populate_redis(data):
    r = redis.Redis()
    r.flushdb()

    start = time.time()

    LOGGER.info('loading into redis')
    for feature in data['features']:
        geoid = feature['properties']['geoid']
        geometry = asShape(feature['geometry'])
        r.set(geoid, geometry.wkt)
    LOGGER.info('loaded into redis [%s]', time.time() - start)


def generate_aggregates(data):
    start = time.time()

    LOGGER.info('generating aggregate JSON file')

    output_data = {}
    for feature in data['features']:
        output_data[feature['properties']['geoid']] = {
            'countyfp': feature['properties']['countyfp'],
            'statefp': feature['properties']['statefp']
        }

    with open('data/census_aggregates.json', 'w') as outfile:
        json.dump(output_data, outfile)

    LOGGER.info('generated aggregate JSON file [%s]', time.time() - start)


def generate_rtree():

    r = redis.Redis()
    dbname = 'data/census.rtree'
    for fname in (dbname + '.dat', dbname +'.idx', ):
        try:
            os.remove(fname)
        except OSError:
            pass

    idx = FastRtree(dbname)

    LOGGER.info('generating rtree index')
    start = time.time()
    for i, geoid in enumerate(r.keys()):
        geom = wkt.loads(r.get(geoid))
        idx.insert(int(geoid), geom.bounds)
        if i % 1000 == 0:
            LOGGER.debug('%s', i)

    LOGGER.info('generated rtree index [%s]', time.time() - start)

if __name__ == '__main__':
    census_data = load_json('data/censustracts.geojson')

    populate_redis(census_data)
    generate_aggregates(census_data)
    generate_rtree()
