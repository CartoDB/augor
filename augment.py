#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import rtree
import sys
import logging
import traceback
import psycopg2
from math import floor, sin, log, pi, radians
from shapely.geometry import Point
from shapely import speedups, wkb

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

assert speedups.available == True
speedups.enable()

NUM_PROCS = multiprocessing.cpu_count()
WRITER = csv.writer(sys.stdout)


class PostgresProcess(multiprocessing.Process):
    '''
    A process with its own connection to postgres as first arg of run method.
    '''

    def __init__(self, *args, **kwargs):
        super(PostgresProcess, self).__init__(*args, **kwargs)

        self.pgres = psycopg2.connect('postgres:///census').cursor()
        args = list(self._args)
        args.insert(0, self.pgres)
        self._args = tuple(args)

# TODO we should read these from config
COLUMNS = '"geoid","geom","b00001001","b00002001","b01001001","b01001002","b01001003","b01001004","b01001005","b01001006","b01001007","b01001008"'

def get_agg_data(pgres, aug_name, id_):
    # TODO should use aug_name, not assume census_extract
    #stmt = 'SELECT * FROM census_extract WHERE ' \
    #        'geoid = \'14000US{}\''.format(str(id_).zfill(11))
    stmt = 'SELECT {} FROM census_extract WHERE ' \
            'geoid = \'14000US{}\''.format(COLUMNS, str(id_).zfill(11))
    pgres.execute(stmt)
    return pgres.fetchone()


def get_headers(pgres, aug_name):
    # TODO should use aug_name, not assume census_extract
    #pgres.execute('SELECT column_name '
    #              'FROM information_schema.columns '
    #              'WHERE table_name = \'census_extract\'')
    #headers = [c[0] for c in pgres.fetchall()]
    headers = [c.replace('"', '') for c in COLUMNS.split(',')]
    headers.extend(('x', 'y', 'q', ))
    return headers


def load_index(aug_name):
    '''
    Load pre-generated rtree index
    '''
    return rtree.Rtree('../data/{}.rtree'.format(aug_name))


def parse_input_csv(itx_q, latIdx, lonIdx, rtree_idx, pgres, aug_name, hashidx):
    reader = csv.reader(sys.stdin)

    for i, row in enumerate(reader):
        if i == 0:
            headers = get_headers(pgres, aug_name)
            blank_row = ['' for _ in headers][2:]
            # TODO we don't want to output headers if we're putting into postgres,
            # this is where we should create our table
            #write_output_csv(row, headers)
            continue

        lat, lon = float(row[latIdx]), float(row[lonIdx])

        hsh = (lat, lon, )
        if hsh in hashidx:
            augs = hashidx[hsh]
            write_output_csv(row, augs[2:])
        else:
            matches = [o for o in rtree_idx.intersection((lon, lat, lon, lat))]
            if len(matches) == 0:
                #LOGGER.warn('no rtree intersection for (lon, lat) %s, %s', lon, lat)
                write_output_csv(row, blank_row)
            else:
                itx_q.put((row, lat, lon, blank_row, matches, ))

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

def augment_row(pgres, itx_q, hashidx, aug_name):
    '''
    Add augmentation columns to this row, checking against actual geometries
    from postgres if necessary.
    '''

    for val in iter(itx_q.get, "STOP"):
        row, lat, lon, blank_row, matches = val
        augs = blank_row

        hsh = (lat, lon, )

        xyq = lonlat2xyq(lat, lon)

        if len(matches) == 1:
            augs = []
            augs.extend(get_agg_data(pgres, aug_name, matches[0]))
            augs.extend(xyq)
            hashidx[hsh] = augs
        else:
            for geoid in matches:
                agg_data = get_agg_data(pgres, aug_name, geoid)
                if agg_data:
                    augs = []
                    augs.extend(agg_data)
                    augs.extend(xyq)
                else:
                    continue

                try:
                    geom = wkb.loads(augs[1].decode('hex'))
                    if geom.contains(Point(lon, lat)):
                        augs.extend(xyq)
                        hashidx[hsh] = augs
                        write_output_csv(row, augs[2:])
                        break  # stop looping the possible shapes
                except Exception as err:
                    LOGGER.warn('Could not process geoid %s: %s',
                                geoid, err)


def write_output_csv(val, augs):
    # TODO deal with multiple augments?
    val.extend(augs)
    WRITER.writerow(val)


def main(latcolno, loncolno, aug_name):
    itx_q = multiprocessing.Queue() # Our intersection job queue

    mgr = multiprocessing.Manager()
    hashidx = mgr.dict()
    pgres = psycopg2.connect('postgres:///census').cursor()

    rtree_idx = load_index(aug_name) # Load rtree index

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [PostgresProcess(target=augment_row,
                              args=(itx_q, hashidx, aug_name))
              for _ in range(NUM_PROCS)]

    try:
        for process in itx_ps:
            process.start() # start each of our intersection processes

        # Start parsing the CSV
        parse_input_csv(itx_q, int(latcolno), int(loncolno), rtree_idx, pgres,
                        aug_name, hashidx)

        for process in itx_ps:
            process.join()
    except BaseException:
        exc_type, exc_value, exc_traceback = sys.exc_info()

        for process in itx_ps:
            process.terminate()

        LOGGER.error(traceback.format_tb(exc_traceback))
        LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    main(*sys.argv[1:])
