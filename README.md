# Augmentation prototype

## Running the script

Install pre-reqs (on mac):

    brew install redis pv spatialindex
    pip install -r requirements.txt

Populate redis and aggregates.  This expects an augmentation CSV with a `geom`
WKT column:

    python prep.py data/augmentation.csv

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
