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


def parse_input_csv(out_q, itx_q, latIdx, lonIdx, rtree_idx):
    reader = csv.reader(sys.stdin)

    for _, row in enumerate(reader):
        lat, lon = float(row[latIdx]), float(row[lonIdx])

        matches = [o for o in rtree_idx.intersection((lon, lat, lon, lat))]
        if len(matches) == 0:
            #LOGGER.warn('no rtree intersection for (lon, lat) %s, %s', lon, lat)
            out_q.put((row, None,))
        elif len(matches) == 1:
            out_q.put((row, matches[0],))
        else:
            itx_q.put((row, lat, lon, matches,))

    for _ in range(NUM_PROCS):
        itx_q.put("STOP")


def augment_row(out_q, itx_q, hashidx, redis_conn):
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
        out_q.put((row, aug,))
    out_q.put("STOP")


def write_output_csv(out_q, agg_index):

    out_csvfile = csv.writer(sys.stdout)

    for _ in range(NUM_PROCS):
        for vals in iter(out_q.get, "STOP"):
            val = vals[0]
            aug = vals[1]
            if aug is None:
                val.extend(['', '', ''])
            else:
                augs = agg_index[str(aug).zfill(11)]
                val.extend([aug, augs['countyfp'], augs['statefp']])
            out_csvfile.writerow(val)


def main(latcolno, loncolno, augmentation):
    itx_q = multiprocessing.Queue() # Our intersection job queue
    out_q = multiprocessing.Queue() # Our output writing queue

    mgr = multiprocessing.Manager()
    hashidx = mgr.dict()
    redis_conn = redis.Redis()

    rtree_idx = load_index(augmentation) # Load rtree index
    agg_index = load_aggregate_index(augmentation) # Load metadata for augmentations

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [multiprocessing.Process(target=augment_row, args=(out_q, itx_q, hashidx, redis_conn, ))
              for _ in range(NUM_PROCS)]

    # Create a process for saving the results
    out_ps = multiprocessing.Process(target=write_output_csv, args=(out_q, agg_index, ))

    out_ps.start() # start listening for results

    for process in itx_ps:
        process.start() # start each of our intersection processes

    # Start parsing the CSV
    parse_input_csv(out_q, itx_q, int(latcolno), int(loncolno), rtree_idx)

    for process in itx_ps:
        process.join()

    out_ps.join()

if __name__ == '__main__':
    main(*sys.argv[1:])
