# postgres-etl-template

Template of an ETL pipeline to load data into a Postgres database.  

The database is designed to combine information 
from user choices (logs) and a music library (songs).
The purpose is to to track user preferences:  
e.g: if a given user prefers to switch between artists or to follow a specific one.


## Design
The framework provides a SQL database with a Star schema, consisting in:
 - 1 Fact table (songplays)
 - 4 Dimension tables (songs, artists, users, time)

## Installation

Before to start:  
Add the current project folder path to PYTHONPATH.  
In ~/.bashrc, append:
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
```
e.g.
```
PYTHONPATH=~/PycharmProjects/postgres-etl-template:$PYTHONPATH 
export PYTHONPATH
```

To install and activate the environment:
```
conda env create -f environment.yml
conda activate postgres_etl_template 
```


## Usage
To drop the current tables and create new empty ones:
```
python postgres_etl_template/src/create_tables.py
```

To run the etl pipeline on the test data:
```
python postgres_etl_template/src/etl.py postgres_etl_template/tests/test_data/song_data  postgres_etl_template/tests/test_data/log_data --reset-table
```

To run the etl pipeline on the full data,  
Download the data from Udacity and run:
```
python postgres_etl_template/src/etl.py path/to/songs/data path/to/logs/data --reset-table
```
e.g:
```
python postgres_etl_template/src/etl.py data/song_data data/log_data --reset-table
```

To check the content of the database, run:
```
python postgres_etl_template/src/check_database.py
```

## Tests
To run all unittests:
```
python -m unittest discover postgres_etl_template/tests
```

