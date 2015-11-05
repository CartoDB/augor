# Augmentation prototype

## Running the script

Install pre-reqs (on mac):

    brew install postgresql postgis pv spatialindex
    pip install -r requirements.txt

Or on Debian flavor:

    sudo apt-get install python-pip python-dev curl pv libspatialindex-dev

And after [adding postgres 9.4 apt
repo](http://www.unixmen.com/install-postgresql-9-4-phppgadmin-ubuntu-14-10/):

    sudo apt-get install postgresql-9.4 postgresql-9.2-postgis-2.1 \
                         postgresql-contrib-9.4 postgresql-9.4-postgis-scripts

To augment a dataset with census data, you'll need census data -- fortunately,
the good people at [censusreporter](https://github.com/censusreporter) have
done a lot of work to load the American Community Survey (ACS) into postgres.
Their blog post about that can be found
[here](http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data).

If the machine is on AWS, you'll want to make sure that postgres is using the
correct EBS volume.

    sudo su -
    /etc/init.d/postgresql stop
    vim /etc/postgresql/9.4/main/postgresql.conf

Edit the file to change `data_directory` to "/mnt/postgresql/9.4/main".  Then:

    mkdir /mnt/postgresql
    mv /var/lib/postgresql/9.4 /mnt/postgresql/
    chown -R postgres:postgres /mnt/postgresql
    /etc/init.d/postgresql start
    exit

First, you'll need to set up a database `census`, with a user `census`.

    sudo su postgres
    createuser -s ubuntu  # for ease of running commands on AWS
    createuser -s census
    createdb census
    exit

Then, you'll need to download the SQL dumps and pipe them into your database.
Make sure you have a lot of free disk space -- once unzipped, the 5-year ACS is
around 160GB, and the TIGER dataset is around 16GB:

    curl https://s3.amazonaws.com/census-backup/acs/2013/acs2013_5yr/acs2013_5yr_backup.sql.gz | gunzip -c | psql -d census

You'll want to make sure that postgis is enabled:

    psql -d census -c 'create extension postgis;'
    curl https://s3.amazonaws.com/census-backup/tiger/2012/tiger2012_backup.sql.gz | gunzip -c | psql -d census

Once you've populated postgres, you need to prep the augmentation flow.  This
means generating a derived table with columns of data we're interested in, as
well as an rtree for faster spatial processing.  You need to provide a data
path where it will create the rtree:

    python prep.py path/to/rtree

Currently, this assumes that the existing user can read, write, and create
tables on the `census` database using trust authentication.

Once the rtree has been created and the postgres table `census_extract` exists
on the `public` schema, we're ready to run an input CSV through augmentation.
`./augment.py` outputs COPY-ready SQL, so you can pipe it directly into `psql`.

Right now, a table is not created automatically for the output, so you'll need
to create it yourself, with appropriate types:

    psql -d census
    # CREATE TABLE t ( ... )

Then you should be able to pipe in the data using COPY:

    cat path/to/input.csv | python augment.py <latcolno> <loncolno> census | \
        psql -c 'COPY t FROM stdin WITH CSV'

If you want to time progress as it's happening and get stats on time for the
process, you can save the input filesize and use `pv`:

    INPUT=path/to/input.csv
    WC=$(wc -l $INPUT)
    FILESIZE=$(echo $WC | cut -d ' ' -f 1)
    time cat $1 | python augment.py <latcolno> <loncolno> census | \
         pv -a -p -e -l -s $FILESIZE | \
         psql -c 'COPY t FROM stdin WITH csv'

Note that it is dependent on having a static input file -- if you're working
with an input stream, you'll need to get the size data from elsewhere to get a
progress meter.

## Using the augmentation pipeline

You'll want to run augmentation from the pipe provided by cerberus:

    cat ../cerberus/pipe | ./augment.sh | psql -c 'COPY t FROM stdin WITH CSV'

