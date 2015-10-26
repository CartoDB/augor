#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# split_geojson.py


# Takes a complete GeoJson dump of all census areas and breaks them into 
# single file json geometries
import json

def main():
    ct = 0
    with open('data/tl_2014_census_tracts_copy.geojson') as data_file:    
        data = json.load(data_file)
        for f in data['features']:
            name = f['properties']['geoid']
            with open('data/census/'+str(name)+'.json', 'w') as outfile:
                json.dump(f['geometry'], outfile)
	        ct+=1


if __name__ == '__main__':
    main()

