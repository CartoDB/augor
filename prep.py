#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Prep an augmentation from a CSV with headers and a WKT column `geom`
'''

import sys
import os
import csv
import logging
import ujson as json
import psycopg2


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

csv.field_size_limit(sys.maxsize)


def create_pgres_table(pgres):
    """
    Create a postgres table with all the data we need

    TODO we should take table names as args too?
    """
    with open('acs_tables.json', 'r') as tables_file:
        table_ids = json.load(tables_file)

    pgres.execute('select table_id, column_id from acs2013_5yr.census_column_metadata where table_id '
                  'in ({})'.format(', '.join("'" + c + "'" for c in table_ids)))

    columns_by_seq = {}
    column_ids = []

    # Determine seq numbers for each column that's relevant.  Joining against
    # the original seq tables is much faster (and less repetitive) than joining
    # against the views based on them
    for table_id, column_id in pgres.fetchall():
        column_ids.append(column_id)
        stmt = 'SELECT view_definition ' \
                'FROM information_schema.views ' \
                'WHERE table_schema = \'acs2013_5yr\' AND ' \
                'table_name = \'{}\''.format(table_id.lower())
        LOGGER.debug(stmt)
        pgres.execute(stmt)
        seq_id = pgres.fetchone()[0].split('FROM')[1].replace(';', '').strip()
        if seq_id not in columns_by_seq:
            columns_by_seq[seq_id] = []
        columns_by_seq[seq_id].append(column_id)

    # Create the census_extract table we'll be using for our augmentations
    pgres.execute('DROP TABLE IF EXISTS census_extract')
    LOGGER.info(pgres.statusmessage)
    pgres.execute('CREATE TABLE census_extract '
                  '(geoid CHARACTER VARYING(40) '
                  ' NOT NULL, '
                  'geom GEOMETRY NOT NULL, {data}, {moe})'
                  .format(
                      data=', '.join(['"{}" DOUBLE PRECISION'.format(cid.lower()) for cid in column_ids]),
                      moe=', '.join(['"{}_moe" DOUBLE PRECISION'.format(cid.lower()) for cid in column_ids]),
                  )
                 )
    LOGGER.info(pgres.statusmessage)
    pgres.execute('INSERT INTO census_extract '
                  'SELECT full_geoid, the_geom '
                  'FROM tiger2012.census_name_lookup '
                  'WHERE sumlevel = \'140\'')
    LOGGER.info(pgres.statusmessage)
    pgres.execute('ALTER TABLE census_extract '
                  ' ADD CONSTRAINT census_extract_pk PRIMARY KEY (geoid)')
    LOGGER.info(pgres.statusmessage)

    for seq_id, column_ids in sorted(columns_by_seq.iteritems()):
        LOGGER.info(seq_id)
        stmt = 'UPDATE census_extract ce SET {setclause} ' \
                'FROM {seq_id} d ' \
                'WHERE ce.geoid = d.geoid'.format(
                    seq_id=seq_id,
                    setclause=', '.join(
                        ['"{cid}" = d."{cid}"'.format(cid=cid.lower())
                         for cid in column_ids
                         # ?? were these dropped by census reporter?
                         if not cid.endswith('.5')
                        ]
                    )
                )
        LOGGER.info(stmt)
        pgres.execute(stmt)
        LOGGER.info(pgres.statusmessage)

    stmt = 'CREATE INDEX ON census_extract USING GIST (geom)'
    LOGGER.info(pgres.statusmessage)
    pgres.execute(stmt)
    LOGGER.info(pgres.statusmessage)

def main():
    with psycopg2.connect('postgres:///census') as conn:
        with conn.cursor() as pgres:
            create_pgres_table(pgres)


if __name__ == '__main__':
    main()
    #LOGGER.error('USAGE: python prep.py')
