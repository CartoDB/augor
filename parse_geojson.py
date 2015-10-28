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


def import_json():
    r = redis.Redis()
    r.flushdb()

    start = time.time()

    LOGGER.info('loading JSON')
    with open('data/censustracts.geojson') as f_handle:
        data = json.load(f_handle)
    LOGGER.info('loaded JSON [%s]', time.time() - start)

    LOGGER.info('loading into redis')
    for feature in data['features']:
        geoid = feature['properties']['geoid']
        geometry = asShape(feature['geometry'])
        r.set(geoid, geometry.wkt)
    LOGGER.info('loaded into redis [%s]', time.time() - start)

#def add_bbox():
#    r = redis.Redis()
#
#    start = time.time()
#
#    LOGGER.info('converting bboxes')
#    for geoid in r.keys():
#        if 'bbox' in geoid:
#            continue
#        geom = wkt.loads(r.get(geoid))
#        bbox = box(*geom.bounds)
#        r.set(geoid + '_bbox', bbox.wkt)
#    LOGGER.info('converted bboxes [%s]', time.time() - start)

def generate_rtree():

    r = redis.Redis()
    dbname = 'data/census.rtree'
    for fname in (dbname + '.dat', dbname +'.idx', ):
        try:
            os.remove(fname)
        except OSError:
            pass

    idx = FastRtree(dbname)
    #idx = index.Index()

    LOGGER.info('generating rtree index')
    start = time.time()
    for i, geoid in enumerate(r.keys()):
        geom = wkt.loads(r.get(geoid))
        idx.insert(int(geoid), geom.bounds)
        if i % 1000 == 0:
            LOGGER.debug('%s', i)

    LOGGER.info('generated rtree index [%s]', time.time() - start)

if __name__ == '__main__':
    #import_json()
    generate_rtree()
