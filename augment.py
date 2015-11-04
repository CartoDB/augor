#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import rtree
import sys
import redis
import logging
import ujson as json
from math import floor, sin, log, pi, radians
from shapely.geometry import Point
from shapely import speedups, wkt

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

assert speedups.available == True
speedups.enable()

NUM_PROCS = multiprocessing.cpu_count()

WRITER = csv.writer(sys.stdout)


def get_agg_data(redis_conn, aug_name, id_):
    return json.loads(redis_conn.get(
        '/'.join([aug_name, str(id_).zfill(11)])
    ))


def load_index(aug_name):
    '''
    Load pre-generated rtree index
    '''
    return rtree.Rtree('../data/{}.rtree'.format(aug_name))


def parse_input_csv(itx_q, latIdx, lonIdx, rtree_idx, redis_conn, aug_name, hashidx):
    reader = csv.reader(sys.stdin)

    for i, row in enumerate(reader):
        if i == 0:
            write_output_csv(row, header=True)
            continue

        lat, lon = float(row[latIdx]), float(row[lonIdx])

        hsh = (lat, lon, )
        if hsh in hashidx:
            augs = hashidx[hsh]
            write_output_csv(row, augs)
        else:
            matches = [o for o in rtree_idx.intersection((lon, lat, lon, lat))]
            if len(matches) == 0:
                #LOGGER.warn('no rtree intersection for (lon, lat) %s, %s', lon, lat)
                write_output_csv(row, None)
            elif len(matches) == 1:
                augs = get_agg_data(redis_conn, aug_name, matches[0])
                del augs['geom']
                hashidx[hsh] = augs
                write_output_csv(row, augs)
            else:
                itx_q.put((row, lat, lon, matches,))

    for _ in range(NUM_PROCS):
        itx_q.put("STOP")

def lonlat2xyq(lat, lon, z=31):
    # Converts a lat, lon to a QuadTree X-Y coordinate and QuadKey (x, y, q)
    lat = lat if lat <= 85.05112878 else 85.05112878
    lat = lat if lat >= -85.05112878 else -85.05112878
    lon = lon if lon <= 180 else 180
    lon = lon if lon >= -180 else -180

    fx = (lon+180.0)/360.0
    sinlat = sin(radians(lat))
    fy = 0.5 - log((1+sinlat)/(1-sinlat)) / (4*pi)

    mapsize = 1<<z

    x = floor(fx*mapsize)
    x = 0 if x < 0 else x
    y = floor(fy*mapsize)
    y = 0 if y < 0 else y

    x = int(x if x < mapsize else (mapsize-1))
    y = int(y if y < mapsize else (mapsize-1))
    q = sum(((x & (1 << i)) << (i)) | ((y & (1 << i)) << (i+1)) for i in range(z))
    return (x, y, q)

def augment_row(itx_q, hashidx, redis_conn, aug_name):
    '''
    Add augmentation columns to this row, checking against actual geometries
    from redis if necessary.
    '''

    for val in iter(itx_q.get, "STOP"):
        row, lat, lon, hits = val
        augs = None

        hsh = (lat, lon, )

        if hsh in hashidx:
            augs = hashidx[hsh]
        else:
            for geoid in hits:
                augs = get_agg_data(redis_conn, aug_name, geoid)
                geom = wkt.loads(augs.pop('geom'))
                if geom.contains(Point(lon, lat)):
                    break  # stop looping the possible shapes
            hashidx[hsh] = augs
        write_output_csv(row, augs)


def write_output_csv(val, augs=None, header=False):
    # TODO should be based off of augment
    if header == True:
        val.extend(['geoid', 'countyfp', 'statefp'])
    elif augs is None:
        val.extend(['', '', ''])
    else:
        val.extend([augs['geoid'], augs['countyfp'], augs['statefp']])
    WRITER.writerow(val)


def main(latcolno, loncolno, aug_name):
    itx_q = multiprocessing.Queue() # Our intersection job queue

    mgr = multiprocessing.Manager()
    hashidx = mgr.dict()
    redis_conn = redis.Redis()

    rtree_idx = load_index(aug_name) # Load rtree index

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [multiprocessing.Process(target=augment_row,
                                      args=(itx_q, hashidx, redis_conn, aug_name))
              for _ in range(NUM_PROCS)]

    for process in itx_ps:
        process.start() # start each of our intersection processes

    # Start parsing the CSV
    parse_input_csv(itx_q, int(latcolno), int(loncolno), rtree_idx, redis_conn, aug_name, hashidx)

    for process in itx_ps:
        process.join()


if __name__ == '__main__':
    main(*sys.argv[1:])
