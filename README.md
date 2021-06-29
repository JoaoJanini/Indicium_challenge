# Data Pipeline

To build the pipeline, I used a python framework called Luigi. Luigi let's you create idempotent task, define their outputs and their dependencies. It was developed by Spotify and it is used by many companies. 

For the database, in order to keep the code consistent, I used also used postgres.

## Setup the databases

Execute this docker compose command to Setup the challenge database and the final query database;

```bash
docker-compose up
```

## Installs the necessary python packages

```bash
pip3 install -r requirements.txt
```

## Executes the whole pipeline from the beggining 

```bash
python3 main.py
```

## Execute step 1 /order_details.csv -> LocalFile only 

Extracts only the csv to the local file. Specify Date to save information for a specifc date.

```python
PYTHONPATH='.' luigi --module stepTwo ExtractFromCSV --local-scheduler
```

## Execute step 1 /Northwind.db tables -> LocalFile only

Extracts the Northwind's db table to the local file. Specify date after to save information for a specifc date.

```python
PYTHONPATH='.' luigi --module stepTwo ExtractFromDatabase --local-scheduler
```

## Execute step 2 only

Executes the second step of the pipeline, extracting the local data, storing it to the new database creating and saving the new query. If the step one was not executed before, it will also run the step one. If the step two runs successfully for a particular day, you will have to delete the query result for that particular date, otherwise luigi treat the task as done when it runs and will not execute it again.

```python
PYTHONPATH='.' luigi --module stepTwo ExtractLocal --local-scheduler
```


## Comments

Alternatively, for every listed step, you could specify the parameter date for any of the tasks listed above.

```python
PYTHONPATH='.' luigi --module stepTwo ExtractLocal --date 2021-10-10 --local-scheduler
```

If the output of a task is found on the file system, the file will not execute again, as the scheduler will consider it as already succeeded.
