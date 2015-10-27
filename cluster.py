#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import json
import math
import time
import sys
import redis
import fileinput
import logging
from shapely.geometry import Point
from shapely import speedups, wkt

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))

assert speedups.available == True
speedups.enable()

NUM_PROCS = multiprocessing.cpu_count()

class QuadTree(object):
    def __init__(self, zoom):
        # Only calculate the quadtree math constants one time
        self.zoom = zoom
        self.tileSize = 256.0
        self.initialResolution = 2.0 * math.pi * 6378137 / self.tileSize
        self.originShift = 2.0 * math.pi * 6378137 / 2.0
        self.res = self.initialResolution / (2**self.zoom);

    def tile_from_lat_lon(self, lat, lon):

        # // "Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:900913"
        mx = lon * self.originShift / 180.0;
        my = math.log( math.tan((90.0 + lat) * math.pi / 360.0 )) / (math.pi / 180.0);
        my = my * self.originShift / 180.0;

        # // "Converts EPSG:900913 to pyramid pixel coordinates in given zoom level"
        px = (mx + self.originShift) / self.res;
        py = (my + self.originShift) / self.res;

        # // "Returns a tile covering region in given pixel coordinates"
        tx = str(int( math.ceil( px / (self.tileSize) ) - 1 ));
        ty = str(int((2**self.zoom) - 1 - int( math.ceil( py / (self.tileSize) ) - 1 )));

        return (tx, ty)

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

        # Load our QuadTree index calculated by GenerateIndex.ipynb
        cindex = self.load_index(self.augmentation)
        self.zoom = cindex['zoom'] # Store the zoom level of the QT index
        self.tileidx = cindex['index'] # The index itself
        self.geom_directory = cindex['geometry_directory'] # The directory of geometries for each focus

        self.qt = QuadTree(self.zoom)

        self.agg_index = self.load_aggregate_index(self.augmentation) # Load our higher geom index map

        # Create a process for parsing the CSV rows
        #self.pin = multiprocessing.Process(target=self.parse_input_csv, args=(fno, ))

        # Create a process for calculating row intersections. Provide it shared memory objects
        self.ps = [ multiprocessing.Process(target=self.augment_row, args=(hashidx,r,))
                        for i in range(self.psprocs)]
        # Create a process for saving the results
        self.pout = multiprocessing.Process(target=self.write_output_csv, args=())

        #self.pin.start() # start processing the CSV
        self.pout.start() # start listening for results

        for p in self.ps:
            p.start() # start each of our intersection processes

        #self.pin.join()
        self.parse_input_csv()

        for p in self.ps:
            p.join()

        self.pout.join()

    def load_index(self, augmentation):
        if augmentation == 'census':
            with open('data/census.json') as data_file:    
                return json.load(data_file)

    def load_aggregate_index(self, augmentation):
        if augmentation == 'census':
            with open('data/census_aggregates.json') as data_file:    
                return json.load(data_file)

    def parse_input_csv(self):
        # Read the input file with mmap and add every row to the queue
        #with open(self.infile, "r+b") as f:

        reader = csv.reader(fileinput.input())
        # read content via standard file methods

        for L, row in enumerate(reader):
            if L == 0 and self.options['skipHeader'] == True:
                self.header = row
                continue

            lat = float(row[self.latIdx])
            lon = float(row[self.lonIdx])

            aug = None

            tile = self.qt.tile_from_lat_lon(lat, lon)

            tx, ty = tile

            if tx in self.tileidx and ty in self.tileidx[tx]:
                if len(self.tileidx[tx][ty])==1:
                    # if the tile only intersects one geom, we are done
                    aug = self.tileidx[tx][ty][0]
                    self.outq.put( (row, aug ) )
                else:
                    self.idx.put( (row, lat, lon, self.tileidx[tx][ty] ) )
            else:
                self.outq.put( (row, None ) )

        for i in range(self.psprocs):
            self.idx.put("STOP")

    def augment_row(self, hashidx, r):

        for val in iter(self.idx.get, "STOP"):
            row = val[0] 
            lat = val[1]
            lon = val[2] 
            hits = val[3] 
            aug = None

            hsh = str(lat)+","+str(lon) # TODO real hash perhaps, not sure if needed

            if hsh in hashidx:
                aug = hashidx[hsh] 
            else:
                for v in hits:
                    geom = wkt.loads(r.get(v))
                    if geom.contains(Point(lon, lat)):
                        aug = v
                        break  # stop looping the possible shapes
                hashidx[hsh] = aug
            self.outq.put( (row, aug ) )
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
        for works in range(self.psprocs):
            for vals in iter(self.outq.get, "STOP"):
                val = vals[0]
                aug = vals[1]
                if aug == None:
                    if self.options['filterNulls'] != True:
                        val.extend(['', '', ''])
                        self.out_csvfile.writerow(val)
                else:
                    augs = self.agg_index[aug]
                    val.extend([aug, augs['countyfp'], augs['statefp']])
                    self.out_csvfile.writerow(val)

def main():
    c = CSVWorker(NUM_PROCS, "census", 5, 6, {'delimiter': ',', 'rowOrder': False, 'skipHeader': True, 'filterNulls': False})
    c.start()

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
