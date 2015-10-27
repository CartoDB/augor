#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# index.py

import csv
import multiprocessing
import optparse
import sys
import mmap
import json
import math
import time
from shapely.geometry import shape, Point, Polygon, MultiPolygon

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
    def __init__(self, numprocs, augmentation, infile, outfile, latIdx, lonIdx, options):

        self.numprocs = numprocs
        self.psprocs = self.numprocs # in case any lesser value is better (n-1)

        self.infile = infile
        self.outfile = outfile

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
        opengeos = mgr.dict()

        # Load our QuadTree index calculated by GenerateIndex.ipynb
        cindex = self.load_index(self.augmentation)
        self.zoom = cindex['zoom'] # Store the zoom level of the QT index
        self.tileidx = cindex['index'] # The index itself
        self.geom_directory = cindex['geometry_directory'] # The directory of geometries for each focus

        self.qt = QuadTree(self.zoom)

        self.agg_index = self.load_aggregate_index(self.augmentation) # Load our higher geom index map
        
        # Create a process for parsing the CSV rows
        self.pin = multiprocessing.Process(target=self.parse_input_csv, args=())
        # Create a process for calculating row intersections. Provide it shared memory objects
        self.ps = [ multiprocessing.Process(target=self.augment_row, args=(hashidx,opengeos,))
                        for i in range(self.psprocs)]
        # Create a process for saving the results
        self.pout = multiprocessing.Process(target=self.write_output_csv, args=())

        self.pin.start() # start processing the CSV
        self.pout.start() # start listening for results

        for p in self.ps:
            p.start() # start each of our intersection processes

        self.pin.join()
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
        with open(self.infile, "r+b") as f:
            # memory-mapInput the file, size 0 means whole file
            mapInput = mmap.mmap(f.fileno(), 0)
            # read content via standard file methods
            L=0
            for row in iter(mapInput.readline, ""):
                if L==0 and self.options['skipHeader'] == True:
                    self.header = row
                    L+=1
                    continue
                # self.idx.put( (L, s) )

                row = row.strip()
                data = row.split(self.options['delimiter'])   
                lat = float(data[self.latIdx])
                lon = float(data[self.lonIdx])

                aug = None

                tile = self.qt.tile_from_lat_lon(lat, lon)

                tx = tile[0]
                ty = tile[1]

                if tx in self.tileidx and ty in self.tileidx[tx]:
                    if len(self.tileidx[tx][ty])==1:
                        # if the tile only intersects one geom, we are done
                        aug = self.tileidx[tx][ty][0]
                        self.outq.put( (row, aug ) )
                    else: 
                        self.idx.put( (row, lat, lon, self.tileidx[tx][ty] ) )
                else:
                    self.outq.put( (row, None ) )


                L+=1
            mapInput.close()

        for i in range(self.psprocs):
            self.idx.put("STOP")

    def augment_row(self, hashidx, opengeos):

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
                    if v in opengeos:
                        # see if the shape exists in shared memory
                        c = opengeos[v]
                    else:
                        d = json.load(open(self.geom_directory+'/%s.json' % v, 'r'))
                        # shapely doesn't seem to love all polys equally
                        try:
                            c = MultiPolygon([Polygon(pol) for pol in d['coordinates']]) 
                        except:
                            c = Polygon(d['coordinates'][0][0])

                    # Store the shape object in shared memory
                    opengeos[v] = c

                    if Point(lon, lat).within(c):
                        aug = v
                        break  # stop looping the possible shapes
                hashidx[hsh] = aug
            self.outq.put( (row, aug ) )
        self.outq.put("STOP")

    def write_output_csv(self):

        outfile = open(self.outfile, "w")
        self.out_csvfile = outfile

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
                if aug==None:
                    if self.options['filterNulls'] != True:
                        self.out_csvfile.write( val + self.options['delimiter'].join(['','','']) + "\n" )
                else:
                    augs = self.agg_index[aug]
                    self.out_csvfile.write( val + self.options['delimiter'].join([aug, augs['countyfp'], augs['statefp']]) + "\n" )
        outfile.close()

def main():
    c = CSVWorker(NUM_PROCS, "census", "data/test.mini.csv", "data/output.csv", 5, 6, {'delimiter': ',', 'rowOrder': False, 'skipHeader': True, 'filterNulls': False})
    c.start()

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
