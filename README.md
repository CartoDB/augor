# Augmentation prototype

### Higher aggregates table

For each census area in the index, we also know what State and County the area is within. In the next iteration we can add ZCTA too. This data is stored as an object in *census_aggregates.json* by default

## Running the script

Install pre-reqs (on mac):

    brew install redis pv spatialindex
    pip install -r requirements.txt

Populate redis (expects census tract json at `data/censustracts.geojson`):

    python parse_geojson.py

Run the script (substituting for `path/to/input.csv`)

    ./run.sh path/to/input.csv > data/output.csv

You will get two progress indicators: one for the file read (which should be
very fast) and the second for output writing (which will just be a kbps
average).

## Using the augmentation pipeline

You'll want to run augmentation from the pipe provided by cerberus:

    cat ../cerberus/pipe | ./augment.sh

This will generate augmented CSVs at `$name.csv` and logs of their creation at
`$name.log`.
