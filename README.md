# Augmentation prototype

## Augmentation Pipeline




## Index builder

GenerateIndex.ipynb creates the static files that will be necessary for the suedo-spatial index. 

There are three critical outputs of the script:

### QuadTree Index

Stored as **data/census.json** by default

The index is simply a QuadTree mapping at a single zoom level of all census tracts. The later search finds the X, Y coordinate of any lat/lon and then searches the index. Here for example is a simplified index,

```json
{"22": {"336": ["02016000100"], "275": ["02180000100"]}}
```

If a lat/lon is converted to X=22 and Y=336, then we know the census area is 02016000100

### Geometries

For every census area id in the index, there is a static geojson file containing the shape. Those files are stored by default the directory *data/census/*

They are split to single census areas to enable only loading the necessary geometries during the script. The assumption is that most datasets will be aggregated over only a small number of census areas.

### Higher aggregates table

For each census area in the index, we also know what State and County the area is within. In the next iteration we can add ZCTA too. This data is stored as an object in *census_aggregates.json* by default
