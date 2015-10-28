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
from shapely.geometry import Point
from shapely import speedups, wkt

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

assert speedups.available == True
speedups.enable()

NUM_PROCS = multiprocessing.cpu_count()

WRITER = csv.writer(sys.stdout)

def load_aggregate_index(augmentation):
    '''
    Load pre-generated JSON mapping of ID to additional columns
    '''
    if augmentation == 'census':
        with open('data/census_aggregates.json') as data_file:
            return json.load(data_file)


def load_index(augmentation):
    '''
    Load pre-generated rtree index
    '''
    if augmentation == 'census':
        return rtree.Rtree('data/census.rtree')


def parse_input_csv(itx_q, latIdx, lonIdx, rtree_idx, agg_idx):
    reader = csv.reader(sys.stdin)

    for _, row in enumerate(reader):
        lat, lon = float(row[latIdx]), float(row[lonIdx])

        matches = [o for o in rtree_idx.intersection((lon, lat, lon, lat))]
        if len(matches) == 0:
            #LOGGER.warn('no rtree intersection for (lon, lat) %s, %s', lon, lat)
            write_output_csv(agg_idx, row)
        elif len(matches) == 1:
            write_output_csv(agg_idx, row, matches[0])
        else:
            itx_q.put((row, lat, lon, matches,))

    for _ in range(NUM_PROCS):
        itx_q.put("STOP")


def augment_row(itx_q, hashidx, redis_conn, agg_idx):
    '''
    Add augmentation columns to this row, checking against actual geometries
    from redis if necessary.
    '''

    for val in iter(itx_q.get, "STOP"):
        row, lat, lon, hits = val
        aug = None

        hsh = (lat, lon, )

        if hsh in hashidx:
            aug = hashidx[hsh]
        else:
            for geoid in hits:
                geom = wkt.loads(redis_conn.get(str(geoid).zfill(11)))
                if geom.contains(Point(lon, lat)):
                    aug = str(geoid).zfill(11)
                    break  # stop looping the possible shapes
            hashidx[hsh] = aug
        write_output_csv(agg_idx, row, aug)


def write_output_csv(agg_idx, val, aug=None):
    if aug is None:
        val.extend(['', '', ''])
    else:
        augs = agg_idx[str(aug).zfill(11)]
        val.extend([aug, augs['countyfp'], augs['statefp']])
    WRITER.writerow(val)


def main(latcolno, loncolno, augmentation):
    itx_q = multiprocessing.Queue() # Our intersection job queue
    #out_q = multiprocessing.Queue() # Our output writing queue

    mgr = multiprocessing.Manager()
    hashidx = mgr.dict()
    redis_conn = redis.Redis()

    rtree_idx = load_index(augmentation) # Load rtree index
    agg_idx = load_aggregate_index(augmentation) # Load metadata for augmentations

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [multiprocessing.Process(target=augment_row,
                                      args=(itx_q, hashidx, redis_conn, agg_idx))
              for _ in range(NUM_PROCS)]

    for process in itx_ps:
        process.start() # start each of our intersection processes

    # Start parsing the CSV
    parse_input_csv(itx_q, int(latcolno), int(loncolno), rtree_idx, agg_idx)

    for process in itx_ps:
        process.join()


if __name__ == '__main__':
    main(*sys.argv[1:])
