#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import rtree
import time
import sys
import redis
import fileinput
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

class CSVWorker(object):
    def __init__(self, numprocs, augmentation, latIdx, lonIdx, options):

        self.numprocs = numprocs
        self.psprocs = self.numprocs # in case any lesser value is better (n-1)

        self.header = ""
        self.latIdx = latIdx
        self.lonIdx = lonIdx
        self.augmentation = augmentation

        self.options = {'delimiter': ',', 'keepRowOrder': False, 'skipHeader': True, 'filterNulls': False}
        self.options.update(options)

    def start(self):

        self.idx = multiprocessing.Queue() # Our intersection job queue
        self.outq = multiprocessing.Queue() # Our output writing queue

        mgr = multiprocessing.Manager()
        hashidx = mgr.dict()
        r = redis.Redis()

        self.rtree = self.load_index(self.augmentation) # Load rtree index
        self.agg_index = self.load_aggregate_index(self.augmentation) # Load metadata for augmentations

        # Create a process for calculating row intersections. Provide it shared memory objects
        self.ps = [ multiprocessing.Process(target=self.augment_row, args=(hashidx,r,))
                        for i in range(self.psprocs)]
        # Create a process for saving the results
        self.pout = multiprocessing.Process(target=self.write_output_csv, args=())

        self.pout.start() # start listening for results

        for p in self.ps:
            p.start() # start each of our intersection processes

        # Start parsing the CSV
        self.parse_input_csv()

        for p in self.ps:
            p.join()

        self.pout.join()

    def load_aggregate_index(self, augmentation):
        if augmentation == 'census':
            with open('data/census_aggregates.json') as data_file:
                return json.load(data_file)

    def load_index(self, augmentation):
        if augmentation == 'census':
            return rtree.Rtree('data/census.rtree')

    def parse_input_csv(self):
        reader = csv.reader(fileinput.input())

        for L, row in enumerate(reader):
            if L == 0 and self.options['skipHeader'] == True:
                self.header = row
                continue

            lat, lon = float(row[self.latIdx]), float(row[self.lonIdx])

            matches = [o for o in self.rtree.intersection((lon, lat, lon, lat))]
            if len(matches) == 0:
                #LOGGER.warn('no rtree intersection for (lon, lat) %s, %s', lon, lat)
                self.outq.put((row, None,))
            elif len(matches) == 1:
                self.outq.put((row, matches[0],))
            else:
                self.idx.put((row, lat, lon, matches,))

        for _ in range(self.psprocs):
            self.idx.put("STOP")

    def augment_row(self, hashidx, r):

        for val in iter(self.idx.get, "STOP"):
            row, lat, lon, hits = val
            aug = None

            hsh = (lat, lon, )

            if hsh in hashidx:
                aug = hashidx[hsh]
            else:
                for geoid in hits:
                    geom = wkt.loads(r.get(str(geoid).zfill(11)))
                    if geom.contains(Point(lon, lat)):
                        aug = str(geoid).zfill(11)
                        break  # stop looping the possible shapes
                hashidx[hsh] = aug
            self.outq.put((row, aug,))
        self.outq.put("STOP")

    def write_output_csv(self):

        self.out_csvfile = csv.writer(sys.stdout)

        # TODO re-enable will NULL filtering etc
        # cur = 0
        # stop = 0
        # buffer = {} 
        # if self.options['rowOrder']==True:
        #     #Keep running until we see numprocs STOP messages
        #     for works in range(self.psprocs):
        #         for val in iter(self.outq.get, "STOP"):
        #             if i != cur:
        #                 buffer[i] = val
        #             else:
        #                 self.out_csvfile.write( val + "\n" )
        #                 cur += 1
        #                 while cur in buffer:
        #                     self.out_csvfile.write( buffer[cur] + "\n" )
        #                     del buffer[cur]
        #                     cur += 1
        # else: 
        for _ in range(self.psprocs):
            for vals in iter(self.outq.get, "STOP"):
                val = vals[0]
                aug = vals[1]
                if aug == None:
                    if self.options['filterNulls'] != True:
                        val.extend(['', '', ''])
                        self.out_csvfile.writerow(val)
                else:
                    augs = self.agg_index[str(aug).zfill(11)]
                    val.extend([aug, augs['countyfp'], augs['statefp']])
                    self.out_csvfile.writerow(val)

def main():
    c = CSVWorker(NUM_PROCS, "census", 5, 6, {'delimiter': ',', 'rowOrder': False, 'skipHeader': True, 'filterNulls': False})
    c.start()

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
