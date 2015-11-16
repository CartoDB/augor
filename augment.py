#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import sys
import logging
import traceback
import psycopg2
import json
import urllib2
from math import floor, sin, log, pi, radians
from itertools import izip_longest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


NUM_PROCS = multiprocessing.cpu_count()
#NUM_PROCS = 1

#COLUMNS = [
#    'geoid',
#    'b01001001',
#    'b01001002',
#    'b01001026',
#    'b03002012',
#    'b03002006',
#    'b03002004',
#    'b03002003',
#    'b09001001',
#    'b09020001',
#    'b11001001',
#    'b14001002',
#    'B15003022',
#    'b15003017',
#    'b17001002',
#    'b19013001',
#    'b22003002',
#    'b23025003',
#    'b23025005'
#]

CHUNK_SIZE = 10


def grouper(iterable, size, fillvalue=None):
    '''
    Collect data into fixed-length chunks or blocks
    '''
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * size
    return izip_longest(fillvalue=fillvalue, *args)


def get_aug_data(pgres, lon, lat):
    '''
    Return augmented data by lon/lat.
    '''
    pgres.execute('execute selectbylonlat(%s, %s)', (lon, lat, ))
    return pgres.fetchone()


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
    stmt = 'DROP TABLE IF EXISTS "{name}";\n'.format(name=tablename)
    LOGGER.debug(stmt)
    sys.stdout.write(stmt)

    columndef = [{
        'name': c['attr'],
        'type': tabletype(c['type'])
    } for c in config['attributes']]

    # TODO how we should actually handle quadkey
    columndef += [{
        'name': 'x',
        'type': 'int8'
    }, {
        'name': 'y',
        'type': 'int8'
    }, {
        'name': 'q',
        'type': 'int8'
    }]

    stmt = 'CREATE TABLE "{name}" ({columns});\n'.format(
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
    for i, col in enumerate(config['attributes']):
        if col['type'] == 'latitude':
            lat_idx = i
        elif col['type'] == 'longitude':
            lon_idx = i
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

    conn = psycopg2.connect('postgres:///census')
    conn.set_isolation_level(0)
    conn.set_session(autocommit=True, readonly=True)

    pgres = conn.cursor()

    # prepare query for the attributes we need
    # TODO should not use `census_extract` table
    columns = [col['augmentation']['code'] for
               col in config['attributes'] if 'augmentation' in col]
    pgres.execute(
        "prepare selectbylonlat as " \
        'SELECT {columns} FROM census_extract ce WHERE ' \
        'geoid LIKE \'14000US%\' AND ({st_within})' \
        .format(columns=', '.join(columns),
                st_within='ST_WITHIN(ST_SetSRID(ST_Point($1, $2), 4326), ce.geom)'))

    #writer = csv.writer(sys.stdout)
    for rows in iter(itx_q.get, "STOP"):

        for row in rows:
            if not row:
                continue
            lat, lon = float(row[lat_idx]), float(row[lon_idx])

            aug_data = get_aug_data(pgres, lon, lat)
            if aug_data:
                row.extend(aug_data)
            else:
                #LOGGER.warn('missing augmentation for row %s', i)
                row.extend([None for _ in columns])

            row.extend(lonlat2xyq(lat, lon))
        out_q.put(rows)


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
    itx_q = multiprocessing.Queue() # Our intersection job queue
    out_q = multiprocessing.Queue() # Our output queue

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
    out_ps = multiprocessing.Process(target=write_rows, args=(out_q,))

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
