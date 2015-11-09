#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import sys
import logging
import traceback
import psycopg2
from math import floor, sin, log, pi, radians
from itertools import izip_longest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


NUM_PROCS = multiprocessing.cpu_count()
#NUM_PROCS = 1

COLUMNS = [
    'geoid',
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

CHUNK_SIZE = 10


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class PostgresProcess(multiprocessing.Process):
    '''
    A process with its own connection to postgres as first arg of run method.
    '''

    def __init__(self, *args, **kwargs):
        super(PostgresProcess, self).__init__(*args, **kwargs)

        conn = psycopg2.connect('postgres:///census')
        conn.set_isolation_level(0)
        conn.set_session(autocommit=True, readonly=True)

        self.pgres = conn.cursor()

        # TODO should use aug_name, not assume census_extract

        # pre-generate select by chunk
        self.pgres.execute(
            "prepare selectbylonlat as " \
            'SELECT {columns} FROM census_extract ce WHERE ' \
            'geoid LIKE \'14000US%\' AND ({st_within})' \
            .format(columns=', '.join(COLUMNS),
                    st_within='ST_WITHIN(ST_SetSRID(ST_Point($1, $2), 4326), ce.geom)'))

        args = list(self._args)
        args.insert(0, self.pgres)
        self._args = tuple(args)


def get_agg_data(pgres, aug_name, lon, lat):
    pgres.execute('execute selectbylonlat(%s, %s)', (lon, lat, ))
    return pgres.fetchone()


def get_aug_headers(pgres, aug_name):
    # TODO should use aug_name, not assume census_extract
    #pgres.execute('SELECT column_name '
    #              'FROM information_schema.columns '
    #              'WHERE table_name = \'census_extract\'')
    #headers = [c[0] for c in pgres.fetchall()]
    headers = ['x', 'y', 'q']
    headers.extend(COLUMNS)
    return headers


def create_output_table(pgres, columns):
    '''
    Create an augmented output table with the specified columns.  Just does
    text for now.
    '''
    stmt = 'DROP TABLE IF EXISTS augmented;'
    LOGGER.info(stmt)
    pgres.execute(stmt)
    pgres.connection.commit()
    stmt = 'CREATE TABLE augmented ({});'.format(
        ', '.join([c + ' TEXT' for c in columns]))
    LOGGER.info(stmt)
    pgres.execute(stmt)
    pgres.connection.commit()


def parse_input_csv(itx_q, out_q, latIdx, lonIdx, pgres, aug_name):
    reader = csv.reader(sys.stdin)

    for rows in grouper(reader, CHUNK_SIZE):
        itx_q.put((rows, latIdx, lonIdx, ))

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


def augment_row(pgres, itx_q, out_q, aug_name):
    '''
    Add augmentation columns to this row, checking against actual geometries
    from postgres if necessary.
    '''

    #writer = csv.writer(sys.stdout)
    for val in iter(itx_q.get, "STOP"):

        rows, latIdx, lonIdx = val

        #lonlats = [(r[lonIdx], r[latIdx]) for r in rows if r]
        #flat_lonlat = [item for sublist in lonlats for item in sublist]
        for i, row in enumerate(rows):
            if not row:
                break
            lat, lon = float(row[latIdx]), float(row[lonIdx])

            row.extend(lonlat2xyq(lat, lon))

            agg_data = get_agg_data(pgres, aug_name, lon, lat)
            if agg_data:
                row.extend(agg_data)
            else:
                LOGGER.warn('missing augmentation for row %s', i)
                row.extend([None for _ in COLUMNS])
            #writer.writerow(row)
        #writer.writerows(rows[0:i])
        out_q.put(rows[0:i])
    out_q.put("STOP")


def write_rows(out_q):
    writer = csv.writer(sys.stdout)
    for rows in iter(out_q.get, "STOP"):
        writer.writerows(rows)
        #for row in rows:
        #    if len(row) < 35:
        #        LOGGER.warn(row)
        #    else:
        #        pass
        #    writer.writerow(row)


def main(latcolno, loncolno, aug_name):
    itx_q = multiprocessing.Queue() # Our intersection job queue
    out_q = multiprocessing.Queue() # Our output queue

    pgres = psycopg2.connect('postgres:///census').cursor()

    reader = csv.reader(sys.stdin)
    headers = reader.next()
    headers.extend(get_aug_headers(pgres, aug_name))
    create_output_table(pgres, headers)

    # Create a process for calculating row intersections. Provide it shared memory objects
    itx_ps = [PostgresProcess(target=augment_row, args=(itx_q, out_q, aug_name))
              for _ in range(NUM_PROCS)]

    out_ps = multiprocessing.Process(target=write_rows, args=(out_q,))

    try:
        for process in itx_ps:
            process.start() # start each of our intersection processes
        out_ps.start()

        # Start parsing the CSV
        parse_input_csv(itx_q, out_q, int(latcolno), int(loncolno), pgres,
                        aug_name)

        for process in itx_ps:
            process.join()
        out_ps.join()
    except BaseException:
        _, _, exc_traceback = sys.exc_info()

        for process in itx_ps:
            process.terminate()
        out_ps.terminate()

        LOGGER.error(traceback.format_tb(exc_traceback))
        LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    main(*sys.argv[1:])
