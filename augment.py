#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import copy
import csv
import multiprocessing
import sys
import logging
import traceback
import psycopg2
from math import floor, sin, log, pi, radians

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


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
COLUMN_DICT = {
    'geoid': 'geoid',
    'geom': 'geom',
    'b01001001': 'population',
    'b01001002': 'male',
    'b01001026': 'female',
    'b03002012': 'hispanic',
    'b03002006': 'asian',
    'b03002004': 'black',
    'b03002003': 'white',
    'b09001001': 'children', # under 18
    'b09020001': 'seniors', # 65 and older
    'b11001001': 'households',
    'b14001002': 'school_enrollment',  # need grade school enrollment, not "detailed",
                                       # this by default includes enrollment in college
                                       # and grad school, not just grade school
    'B15003022': 'bachelors', # would we want to assume people with
                              # masters/doctorate have a bachelors too?  they would be excluded...
    #'': 'no_high_school', # tough.. do we cut off at grade 9 on B15003?
    # high school diploma vs. not?
    # b150003002 + b150003003 + b150003004 + b150003005 +
    # b150003006 + b150003007 + b150003008 + b150003009 +
    # b150003010 + b150003011 + b150003012 + b150003013 +
    # b150003014 + b150003015 + b150003016
    'b15003017': 'high_school', # those with high school diplomas only?
                                # for GED too:  b150003017 + b150003018
    'b17001002': 'poverty',
    'b19013001': 'hhi',
    'b22003002': 'food_stamps', # denominator of this is # of households
    'b23025003': 'civilian_labor_force',
    'b23025005': 'unemployment', # denominator of this is civilian labor force
    #'': 'uninsured' # #%*&# is broken by age before status:
    #'b27001005': 'uninsured',
    #'b27001008': 'uninsured',
    #'b27001011': 'uninsured',
    #'b27001014': 'uninsured',
    #'b27001017': 'uninsured',
    #'b27001020': 'uninsured',
    #'b27001023': 'uninsured',
    #'b27001026': 'uninsured',
    #'b27001029': 'uninsured',
    #'b27001033': 'uninsured',
    #'b27001036': 'uninsured',
    #'b27001039': 'uninsured',
    #'b27001042': 'uninsured',
    #'b27001045': 'uninsured',
    #'b27001048': 'uninsured',
    #'b27001051': 'uninsured',
    #'b27001054': 'uninsured',
    #'b27001057': 'uninsured'

}
COLUMNS = [
    'geoid',
    'geom',
    'b01001001',
    'b01001002',
    'b01001026',
    'b03002012',
    'b03002006',
    'b03002004',
    'b03002003',
    'b09001001',
    'b09020001',
    'b11001001',
    'b14001002',
    'B15003022',
    'b15003017',
    'b17001002',
    'b19013001',
    'b22003002',
    'b23025003',
    'b23025005'
]

def get_agg_data(pgres, aug_name, lat, lon):
    # TODO should use aug_name, not assume census_extract
    #stmt = 'SELECT * FROM census_extract WHERE ' \
    #        'geoid = \'14000US{}\''.format(str(id_).zfill(11))
    stmt = 'SELECT {columns} FROM census_extract ce WHERE ' \
           'geoid LIKE \'14000US%\' AND ' \
           'ST_WITHIN(ST_SetSRID(ST_Point({lon}, {lat}), 4326), ce.geom)'.format(
               columns=', '.join(COLUMNS),
               lon=lon,
               lat=lat)
    pgres.execute(stmt)
    return pgres.fetchone()


def get_headers(pgres, aug_name):
    # TODO should use aug_name, not assume census_extract
    #pgres.execute('SELECT column_name '
    #              'FROM information_schema.columns '
    #              'WHERE table_name = \'census_extract\'')
    #headers = [c[0] for c in pgres.fetchall()]
    headers = copy.copy(COLUMNS)
    headers.extend(('x', 'y', 'q', ))
    return headers


def create_output_table(pgres, columns):
    '''
    Create an augmented output table with the specified columns.  Just does
    text for now.
    '''
    stmt = 'CREATE TABLE IF NOT EXISTS augmented ({});'.format(
        ', '.join([c + ' TEXT' for c in columns]))
    LOGGER.info(stmt)
    pgres.execute(stmt)
    pgres.connection.commit()


def parse_input_csv(itx_q, latIdx, lonIdx, pgres, aug_name, hashidx):
    reader = csv.reader(sys.stdin)

    for i, row in enumerate(reader):
        if i == 0:
            headers = get_headers(pgres, aug_name)
            blank_row = ['' for _ in headers][2:]
            # TODO we don't want to output headers if we're putting into postgres,
            # this is where we should create our table
            #write_output_csv(row, headers)
            row.extend(headers)
            create_output_table(pgres, row)
            continue

        lat, lon = float(row[latIdx]), float(row[lonIdx])

        hsh = (lat, lon, )
        if hsh in hashidx:
            augs = hashidx[hsh]
            write_output_csv(row, augs[2:])
        else:
            itx_q.put((row, lat, lon, blank_row, ))

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
        row, lat, lon, blank_row = val
        augs = blank_row

        hsh = (lat, lon, )

        xyq = lonlat2xyq(lat, lon)

        augs = []
        agg_data = get_agg_data(pgres, aug_name, lat, lon)
        if agg_data:
            augs.extend(agg_data)
            augs.extend(xyq)
            hashidx[hsh] = augs
            write_output_csv(row, augs[2:])
        else:
            pass


def write_output_csv(val, augs):
    # TODO deal with multiple augments?
    val.extend(augs)
    WRITER.writerow(val)


def main(latcolno, loncolno, aug_name):
    itx_q = multiprocessing.Queue() # Our intersection job queue

    mgr = multiprocessing.Manager()
    hashidx = mgr.dict()
    pgres = psycopg2.connect('postgres:///census').cursor()

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [PostgresProcess(target=augment_row,
                              args=(itx_q, hashidx, aug_name))
              for _ in range(NUM_PROCS)]

    try:
        for process in itx_ps:
            process.start() # start each of our intersection processes

        # Start parsing the CSV
        parse_input_csv(itx_q, int(latcolno), int(loncolno), pgres,
                        aug_name, hashidx)

        for process in itx_ps:
            process.join()
    except BaseException:
        _, _, exc_traceback = sys.exc_info()

        for process in itx_ps:
            process.terminate()

        LOGGER.error(traceback.format_tb(exc_traceback))
        LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    main(*sys.argv[1:])
