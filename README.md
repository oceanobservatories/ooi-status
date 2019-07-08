# OOI Status

This project contains code to automate the monitoring of data ingestion in the OOI system.

## Installation

To install this project for development (pip):

```commandline
mkvirtualenv status  # need to follow the instructions for installing virtualenv first
yum install postgresql-devel
pip install -U pip
pip install -r requirements.txt
```

To install this project for development (conda):

```commandline
conda env create -f conda_env.yml
```

To install this project for test/production (pip):

```commandline
mkvirtualenv ooi_status
pip install -U pip
pip install -r requirements.txt
pip install .
```

To install this project for test/production (conda):

```commandline
conda config --append channels ooi
conda config --append channels conda-forge
conda create -n status ooi-status
```

## Running

The project has two runnable services, the status monitor backend and a corresponding HTTP API service which allows
the backend to be configured and supports querying various status-related items. To run the backend:

```commandline
ooi_status_monitor
```

The default configuration can be overridden by providing a fully-qualified path in the environmental variable
OOISTATUS_SETTINGS. For example:

```commandline
export OOISTATUS_SETTINGS=$(pwd)/local_config.py
ooi_status_monitor
```

local_config.py

```python
MONITOR_URL ='postgresql+psycopg2://user@localhost/monitor'
METADATA_URL = 'postgresql+psycopg2://user@localhost/metadata'
```

And to run the HTTP API service (accepts same settings override as described for the backend monitor):  
NOTE: this can be accomplished by executing run_gunicorn.sh from the root folder

```commandline
PSYCOGREEN=true gunicorn -w 2 -k gevent -b 0.0.0.0:9000 ooi_status.api:app
```

See the gunicorn documentation for more information on the various options available for gunicorn.

## Stopping/starting ooi-status using Conda

The following command is used to determine if ooi-status is running:

```commandline
ps -fewH | grep "9000 ooi_status" | grep -v grep
```

If ooi-status is running you will see results similar to the following (the 1st process is the parent):

```commandline
asadev    91877  91876  0  2018 ?        00:10:48     /home/asadev/miniconda2/envs/ooi_status/bin/python /home/asadev/miniconda2/envs/ooi_status/bin/gunicorn --log-config logging.conf -w 2 -k gevent -b 0.0.0.0:9000 ooi_status.api:app
asadev    89288  91877  0 Jun19 ?        00:04:37       /home/asadev/miniconda2/envs/ooi_status/bin/python /home/asadev/miniconda2/envs/ooi_status/bin/gunicorn --log-config logging.conf -w 2 -k gevent -b 0.0.0.0:9000 ooi_status.api:app
asadev    89289  91877  0 Jun19 ?        00:04:33       /home/asadev/miniconda2/envs/ooi_status/bin/python /home/asadev/miniconda2/envs/ooi_status/bin/gunicorn --log-config logging.conf -w 2 -k gevent -b 0.0.0.0:9000 ooi_status.api:app
```

To kill all the processes, kill the parent and the children will die along with it. A shortcut to killing all of them:

```commandline
for i in $(ps -eo ppid,pid,cmd | grep 9000 | grep -v grep | awk '{print $1, $2}'); do echo $i; done | sort | uniq -c | awk '$1 == 3 {print "kill",$NF}' | bash
```

To start ooi-status run the following commands:

```commandline
conda activate ooi_status
run_gunicorn.sh > /dev/null 2>&1 & (NOTE: if . is not on PATH, then prepend "./" to script)
conda deactivate
```

Confirm ooi-status is started properly by running the command listed at the start of this section and verify it shows 3 new processes similar to the ones listed right after that command.

## DDL Generation

This project uses alembic to track DDL changes between revisions. These DDL changes can be applied directly
by alembic (online mode) or alembic can generate SQL to be executed via psql. To upgrade (or create) your
database in online mode:

```commandline
pip install alembic
alembic upgrade head
```

To generate SQL in offline mode (entire schema):

```commandline
alembic upgrade head --sql
```

Or, you can generate changes between specific revisions:

```commandline
alembic upgrade revisionA:revisionB --sql
```

When run in online mode, alembic will query the database for the current revision and make all DDL changes necessary
to reach the specified version (upgrade or downgrade).

## Populating Expected Stream data from CSV


The ooi_status_monitor executable also provides the ability to load the expected stream definitions from a CSV
file as follows:

```commandline
export OOISTATUS_SETTINGS=$(pwd)/local_config.py
ooi_status_monitor --expected=/path/to/expected.csv
```

## Data Availability

A data availability report, in JSON format, may be obtained by querying the HTTP API service.
The query string is

```commandline
http://<host name>:9000/available/<reference designator>
```

For example

```commandline
http://localhost:9000/available/GP05MOAS-GL523-04-CTDGVM000
```

This will return a JSON object similar to

```JSON
{
  "availability": [
    {
      "categories": {
        "Deployment: 1": {
          "color": "#0073cf"
        }
      },
      "data": [
        [
          "2015-06-02 04:40:00",
          "Deployment: 1",
          "2016-08-28 00:00:00"
        ]
      ],
      "measure": "Deployments"
    },
    {
      "categories": {
        "Missing": {
          "color": "#d9534d"
        },
        "Not Expected": {
          "color": "#ffffff"
        },
        "Present": {
          "color": "#5cb85c"
        },
        "Sparsity Level 1": {
          "color": "#7bcb7b"
        },
        "Sparsity Level 2": {
          "color": "#90d890"
        },
        "Sparsity Level 3": {
          "color": "#ace9ac"
        }
      },
      "data": [
        [
          "2015-06-02 04:40:00",
          "Present",
          "2015-06-07 08:52:29"
        ],
        [
          "2015-06-07 08:52:29",
          "Missing",
          "2015-06-14 03:50:32"
        ],
        [
          "2015-06-14 03:50:32",
          "Present",
          "2015-08-29 00:01:20"
        ]
      ],
      "measure": "recovered_host ctdgv_m_glider_instrument_recovered"
    }
  ]
}
```

Note: an actual query may return additional data points and additional streams.

## Interpreting Data Availability

Data availability is identified two different ways:

1. Actual gaps in data. Data Gaps are identified by computing the time gaps between consecutive bins used to store the data in Cassandra. A time gap greater than 0.1% of the deployment time is reported as having missing data.
2. Relative sparsity of data in Cassandra bins. Sparse Data is identified by computing the average time separation between data points in Cassandra bin. Bins having an average time separation greater than 0.1% of the deployment time are reported as sparse.

### Interpretation of Data Availability Colors

Data Availability is reported as a Tool Tip string with a color value for ease of display.

| Tool Tip         |  Color  | Description                                                                                                       |
| ---------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| Not Expected     | #FFFFFF | No data are expected in the time interval |
| Present          | #5CB85C | Average time between data points is less than or equal to the average time separation over the entire data set |
| Sparsity Level 1 | #7BCB7B | Average time between data points is between 100% and 150% of the average time sepatation over the entire data set |
| Sparsity Level 2 | #90D890 | Average time between data points is between 150% and 200% of the average time sepatation over the entire data set |
| Sparsity Level 3 | #ACE9AC | Average time between data points is greater than 200% of the average time sepatation over the entire data set |
| Missing          | #D9534D | There are no data available for the time interval |

### Data Availability Display Configuration

Data Availability colors and sparsity bounds are configured in default_settings.py
