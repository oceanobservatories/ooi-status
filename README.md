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
