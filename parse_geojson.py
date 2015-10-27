#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Convert geojson of census tracts into a dict that can be quickly searched
'''

import ujson as json
import logging
import time
import sys
from shapely.geometry import asShape, box
from shapely import speedups, wkt
import redis

assert speedups.available == True
speedups.enable()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

def import_json():
    r = redis.Redis()

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

def add_bbox():
    r = redis.Redis()

    start = time.time()

    LOGGER.info('converting bboxes')
    for geoid in r.keys():
        geom = wkt.loads(r.get(geoid))
        bbox = box(*geom.bounds)
        r.set(geoid + '_bbox', bbox.wkt)
    LOGGER.info('converted bboxes [%s]', time.time() - start)

if __name__ == '__main__':
    import_json()
    add_bbox()
