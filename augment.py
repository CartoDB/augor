#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# augment.py

import csv
import multiprocessing
import sys
import logging
import traceback
import psycopg2
import json
import urllib2
import os
import operator
from math import floor, sin, log, pi, radians
from itertools import izip_longest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


NUM_PROCS = multiprocessing.cpu_count()
#NUM_PROCS = 1

CHUNK_SIZE = 50
SELECT_COLUMNS = ', '.join(['%s' for _ in xrange(0, CHUNK_SIZE*2)])


def grouper(iterable, size, fillvalue=None):
    '''
    Collect data into fixed-length chunks or blocks
    '''
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * size
    return izip_longest(fillvalue=fillvalue, *args)


def get_aug_data(pgres, lonlats):
    '''
    Return augmented data by lon/lat.
    '''
    if len(lonlats) < CHUNK_SIZE * 2:
        lonlats.extend([None for _ in xrange(CHUNK_SIZE * 2 - len(lonlats))])
    pgres.execute('execute selectbylonlat({})'.format(SELECT_COLUMNS), lonlats)
    return pgres


def tabletype(configtype):
    '''
    Convert a config column type to something usable in postgres.  Hopefully
    can be eliminated eventually.
    '''
    if configtype in ('longitude', 'latitude'):
        return 'float'
    return configtype


def create_output_table(config):
    '''
    Create an augmented output table with the specified columns.
    Prints commands needed to COPY into this new table to STDOUT.
    '''
    tablename = config['table']['name']
    stmt = 'SET statement_timeout=0; DROP TABLE IF EXISTS "{name}";\n'.format(name=tablename)
    LOGGER.debug(stmt)
    sys.stdout.write(stmt)

    columndef = [{
        'name': c['attr'],
        'type': tabletype(c['type'])
    } for c in config['attributes'] + config['augmentations']]

    # TODO how we should actually handle quadkey
    columndef += [{
        'name': 'quadkey_x',
        'type': 'int4'
    }, {
        'name': 'quadkey_y',
        'type': 'int4'
    }, {
        'name': 'quadkey',
        'type': 'int8'
    }]

    stmt = 'CREATE UNLOGGED TABLE "{name}" ({columns});\n'.format(
        name=tablename,
        columns=', '.join(['{name} {type}'.format(**c) for c in columndef])
    )
    LOGGER.debug(stmt)
    sys.stdout.write(stmt)

    stmt = 'COPY {name} FROM stdin WITH CSV;\n'.format(name=tablename)
    LOGGER.debug(stmt)
    sys.stdout.write(stmt)


def find_lon_lat_column_idxs(config):
    '''
    Determine lon/lat column indexes from config file. Returns tuple (lon_idx,
    lat_idx).
    '''
    for col in config['attributes']:
        if col['type'] == 'latitude':
            lat_idx = col['csv']
        elif col['type'] == 'longitude':
            lon_idx = col['csv']
    return (lon_idx, lat_idx)


def parse_input_csv(itx_q, config):
    '''
    Read input CSV, sending chunks of rows to the itx_q and telling them to
    stop when everything is done reading.
    '''
    reader = csv.reader(sys.stdin,
                        delimiter=str(config.get('csv', {}).get('separator', ',')))

    for rows in grouper(reader, CHUNK_SIZE):
        itx_q.put(rows)

    for _ in range(NUM_PROCS):
        itx_q.put("STOP")


def lonlat2xyq(lat, lon, z=31):
    '''
    Converts a lat, lon to a QuadTree X-Y coordinate and QuadKey (x, y, q)
    '''
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


def augment_row(itx_q, out_q, lon_idx, lat_idx, config):
    '''
    Add augmentation columns to this row, checking against actual geometries
    from postgres if necessary.
    '''

    conn = psycopg2.connect('postgres://{user}:{password}@{host}:{port}/{dbname}'.format(
        user=os.environ.get('PGUSER', ''),
        password=os.environ.get('PGPASS', ''),
        host=os.environ.get('PGHOST', ''),
        port=os.environ.get('PGPORT', ''),
        dbname=os.environ.get('PGDATABASE', '')
    ))
    conn.set_isolation_level(0)
    conn.set_session(autocommit=True, readonly=True)

    pgres = conn.cursor()

    # prepare query for the attributes we need
    # TODO should not use `census_extract` table
    augmentation_columns = [col['augmentation']['code'] for col in config['augmentations']]
    pgres.execute(
        "PREPARE selectbylonlat as " \
        'SELECT {columns} FROM (VALUES {values}) t (lat, lon) ' \
        'LEFT JOIN census_extract ce ' \
        'ON ST_WITHIN(ST_SetSRID(ST_Point(lon::FLOAT, lat::FLOAT), 4326), ce.geom) ' \
        'WHERE geoid IS NULL OR geoid LIKE \'14000US%\' ' \
        .format(values=', '.join(['(${}, ${})'.format(i*2+1, i*2+2) for i in xrange(0, CHUNK_SIZE)]),
                columns=', '.join(augmentation_columns)))

    # Determine name and order of input columns from a config
    csv_columns = [col['csv'] for col in config['attributes']]

    #writer = csv.writer(sys.stdout)
    for rows in iter(itx_q.get, "STOP"):

        out_rows = []
        lonlats = [ll for row in rows if row
                   for ll in (float(row[lat_idx]), float(row[lon_idx]))]  # flatten lonlats
        for i, aug_data in enumerate(get_aug_data(pgres, lonlats)):
            row = rows[i]
            if not row:
                continue
            lat, lon = float(row[lat_idx]), float(row[lon_idx])
            out_row = list(operator.itemgetter(*csv_columns)(row))

            if aug_data:
                out_row.extend(aug_data)
            else:
                LOGGER.warn('missing augmentation for row %s', i)
                out_row.extend([None for _ in augmentation_columns])

            out_row.extend(lonlat2xyq(lat, lon))
            out_rows.append(out_row)

        out_q.put(out_rows)


def write_rows(out_q):
    '''
    Read from output queue and write til we're done.
    '''
    writer = csv.writer(sys.stdout)

    for rows in iter(out_q.get, "STOP"):
        for row in rows:
            if row:
                writer.writerow(row)


def get_config(config_uri):
    """
    Obtain config details from URL
    """
    if config_uri.startswith('http'):
        return json.loads(urllib2.urlopen(config_uri).read())
    else:
        with open(config_uri, 'r') as config_file:
            config = json.load(config_file)
        return config


def main(config_url):
    '''
    Process a stream of data from STDIN based off the JSON config found at
    `config_url`.
    '''
    itx_q = multiprocessing.Queue(32767) # Our intersection job queue
    out_q = multiprocessing.Queue(32767) # Our output queue

    config = get_config(config_url)
    create_output_table(config)

    reader = csv.reader(sys.stdin)
    if config.get('csv', {}).get('header'):
        reader.next()  # Skip header row, by default does not

    lon_idx, lat_idx = find_lon_lat_column_idxs(config)

    # Create processes for calculating row intersections
    itx_ps = [multiprocessing.Process(target=augment_row,
                                      args=(itx_q, out_q, lon_idx, lat_idx, config))
              for _ in range(NUM_PROCS)]

    # Create process for output
    out_ps = multiprocessing.Process(target=write_rows, args=(out_q, ))

    try:
        for process in itx_ps:
            process.start() # start each of our intersection processes
        out_ps.start()

        # Start parsing the CSV
        parse_input_csv(itx_q, config)

        for process in itx_ps:
            process.join()
        out_q.put("STOP")
        out_ps.join()
        sys.stdout.write('\\.\n')
    except BaseException:
        _, _, exc_traceback = sys.exc_info()

        for process in itx_ps:
            process.terminate()
        out_ps.terminate()

        LOGGER.error(traceback.format_tb(exc_traceback))
        LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    main(*sys.argv[1:])
