#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# benchmark.py

import csv
import mmap
import pandas as pd
from numpy import genfromtxt, loadtxt
import time


lines = 0
def do_something(line):
    lines = lines=1


def pandas_parse_file(infile, latIdx, lonIdx, delimiter):
    chunksize = 10 ** 4
    # for chunk in :
    L = 0
    C = 0
    for chunk in pd.read_csv(infile, chunksize=chunksize, sep=delimiter):
        for i, s in enumerate(chunk.values):
            # header is a necessary operation
            if L==0 and C==0:
                header = s
                L+=1
                continue
            lat = s[latIdx]
            lon = s[lonIdx]
            do_something( (L, s, lat, lon) )
            L+=1
        C+=1

def gentxt_parse_file(infile, latIdx, lonIdx, delimiter):
    datareader = genfromtxt(infile,delimiter=delimiter)
    L = 0
    for s in datareader:
        # header is a necessary operation
        if L==0:
            header = s
            L+=1
            continue
        lat = s[latIdx]
        lon = s[lonIdx]
        do_something( (L, s, lat, lon) )
        L+=1

def loadtxt_parse_file(infile, latIdx, lonIdx, delimiter):
    datareader = loadtxt(infile, delimiter=delimiter, dtype=str)
    L = 0
    for s in datareader:
        # header is a necessary operation
        if L==0:
            header = s
            L+=1
            continue
        lat = s[latIdx]
        lon = s[lonIdx]
        do_something( (L, s, lat, lon) )
        L+=1

def csv_parse_file(infile, latIdx, lonIdx, delimiter):
    with open(infile, "rb") as csvfile:
        datareader = csv.reader(csvfile, delimiter=delimiter)
        L = 0
        for s in datareader:
            # header is a necessary operation
            if L==0:
                header = s
                L+=1
                continue
            lat = s[latIdx]
            lon = s[lonIdx]
            do_something( (L, s, lat, lon) )
            L+=1

def mmap_parse_file(infile, latIdx, lonIdx, delimiter):
    # Read the input file with mmap and add every row to the queue
    with open(infile, "r+b") as f:
        # memory-mapInput the file, size 0 means whole file
        mapInput = mmap.mmap(f.fileno(), 0)
        # read content via standard file methods
        L=0
        for s in iter(mapInput.readline, ""):
            # header is a necessary operation
            if L==0:
                header = s
                L+=1
                continue
            s = s.split(delimiter)
            lat = s[latIdx]
            lon = s[lonIdx]
            do_something( (L, s, lat, lon) )
            L+=1
        mapInput.close()


def main():
    testfile = "../data/public.csv"
    latIdx = 5
    lonIdx = 6
    delimiter = ','
    start_time = time.time()
    pandas_parse_file(testfile, latIdx, lonIdx, delimiter)
    print("pandas", "--- %s seconds ---" % (time.time() - start_time))

    testfile = "../data/public.csv"
    start_time = time.time()
    mmap_parse_file(testfile, latIdx, lonIdx, delimiter)
    print("mmap", "--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    csv_parse_file(testfile, latIdx, lonIdx, delimiter)
    print("csv", "--- %s seconds ---" % (time.time() - start_time))

    # tooooo slow
    # start_time = time.time()
    # loadtxt_parse_file(testfile)
    # print("gentxt", "--- %s seconds ---" % (time.time() - start_time))

    # tooooo slow
    # start_time = time.time()
    # gentxt_parse_file(testfile)
    # print("loadtxt", "--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()
