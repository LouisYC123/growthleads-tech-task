# Growthleads Tech Task

## Prerequisites
 - Docker

## Setup
 - clone this repo and cd into growthleads-tech-task:  
 ```
 git clone https://github.com/LouisYC123/growthleads-tech-task.git
 cd growthleads-tech-task
 ```
- add the following .env file to the docker directory
    ```
        POSTGRES_USER=<choose_a_username>
        POSTGRES_PASSWORD=<choose_a_password>
        PGADMIN_LISTEN_PORT=5050  
    ```
- build the image for the AWS MWAA local env by running the below (use mwaa-loal-env-win if running on windows):  
    ```./mwaa-local-env build-image```
- Building the image will take a few minutes. When it is done, start the MWAA local runner:  
    ```./mwaa-local-env start```
- Open the Airflow UI in a browser at:  
    ``` localhost:8080/home ```
- Login using the credentials 'admin' and 'test' 
- Head to Admin -> connections and add the following connection:  
    ```
    connection_id: postgres_conn_id
    host: pg_container
    database: growthleads
    login:  <the username you chose in the above .env file>
    password: <the password you chose in the above .env file>
    port: 5432
    ```
- pip install dbt-core, then `cd dbt/growth_leads` and run `dbt deps`  
    - Note: This last step is a temporary workaround as I couldnt get airflow to automatically run dbt deps

## Usage
- The pipeline assumes an architecture that has a data landing zone (such as an S3 bucket in a production environment) and an external process that dumps data in that landing zone each day.
- You will see a landing_zone directory has been created in ```data/```, you can place data files there to replicate a daily data dump.
- You can configure or add new data sources in ```plugins/growthleads_etl/data_sources```. If you add sources here, airflow will automatically generate new tasks to process them.
- Once you start the mwaa local env, the dags will be up and running and waiting for data. The pipeline will start once you add data to the landing zone
- How the architecture addresses the task assignments can be found below (`Notes on assignment tasks` section)

#### Pipeline
- The pipeline follows the ELT paradigm, with extract group tasks performing very minimal processing before loading to a bronze layer database schema
- Extract tasks are configured via the data_sources module in ```plugins/growthleads_etl/```  
    - here you can add/remove or configure data sources
    - To add a new data source, create a schema and add to data_sources
    - You can assign a data source as optional by marking ```if_missing: skip```  
        - (the 'manual' data source is currently configure in this way)
- Once the data is loaded to the database, dbt transforms the data using sql
    - a medallion architecture (bronze, silver, gold) is followed, otherwise known as sources, staging, marts, with the gold (marts) layer being the presentation layer accessed by stakeholders.
- The evvent data in the landing zone is then moved to the archive_zone.  
    - Only event data is archived, the Slowly Changing Dimension data is not.  
        - This can be configured in the data_sources.py module. 
- Data quality is managed using dbt tests, and configured in the schema.yml (see notes section below regarding data quality and unit testing)  

Example dag:  
<img width="1702" alt="airflow_dag_diagram" src="https://github.com/user-attachments/assets/91e79648-941c-4240-92a9-f65cb0db90e6">


## Notes on assignment tasks  
#### Part 1A  
- the pipeline is configure to run daily and will wait for data to arrive in the landing zones
- The results are saved in data marts (views) prefixed with ```solution_1_```

#### Part 1B
- To recalculate commisions from corrected data, add 'manual' and 'voluum' to EXCLUDE_DATA_SOURCES in webtraffic_dag.py and add the corrected data to the landing zone and trigger the dag.  
- dbt's incremental strategy ```delete+insert``` will handle the updating of data in the database using the `source_id` (hash of filename and source) as the unique key.
- recalculated results will appear in the gold layer data mart
- this is true for both the `routy` events data and any slowly changing dimension data (such as `deals.csv`)
- logs are saved in `logs/`  
- to add recalculated `deals` data, add `routy` to EXCLUDE_DATA_SOURCES to exclude all events data and then replace `deals.csv` in the landing_zone with the corrected data. (Only 'routy' needs to be added to the EXCLUDE_DATA_SOURCES list as SCD's are refreshed on every run anyway)

**Note:** For the data from the 27th, the pipeline will wait 6 hours for the manual data. You can either change this in `plugins/growthleads_etl/config.py` or manually mark the `wait_for_manual_csv` task as failed in the UI during the pipeline run.  

#### Part 2
- The `scrapers_dag` follows the same general workflow, except it's `EXCLUDE_DATA_SOURCES` is configured to only process the `scrapers` and `voluum` data sets.
- add both scrapers and voluum data to the landing_zone
- results are saved in data marts prefixed with ```solution_2_```
#### Part 3
- The `combine_results_dag` has dependencies on the above two dags, and triggers two dbt models.
- results are saved in data marts prefixed with ```solution_3_```


## Data Lineage 
<img width="1708" alt="data lineage" src="https://github.com/user-attachments/assets/d553a2a6-6bdc-4567-8aa4-2445434e85b4">


## Relational Model
![relational_model](https://github.com/user-attachments/assets/b342d332-10ae-49f4-831f-6b885d68b366)


## Data Quality management
The following data quality is enforced:  
Python:  
    - dates are cleaned and standardised
SQL:  
- NULLs in dates are replaced with date from filename, assigned to 00:00:00
    - as the lowest granularity we are interested in is daily counts, it was assumed that NULLs could be replaced with filename date + midnight times
- NULL checks are peformed by dbt and configured in `silver/schema.yml`
- Accepted value checks are peformed by dbt and configured in `silver/schema.yml`
- Unique value checks (for dimensional tables) are peformed by dbt and configured in `silver/schema.yml`
- avoidance of same source + filename is enforced by assigning a source_id and using dbt's 'delete+insert' incremental strategy  

## Notes
 - Due to the nature of the sampled data, and joins on `event_time`, it appears that solution 2 totals are always zero.  
 - if you want to run dbt outside of airflow, you have to update 'host' in the dbt/profiles.yml to = 'localhost'.  
 - Postgres schemas (bronze, silver and gold) are created via a startup script in `docker/db-init-scripts`.  

## Future Development
With some more development, I would concentrate on the following features:  
- Rows that fail data quality checks to be sent to seperate table for review (plus notifications / alerts of failures)
    - This can be done with the Great Expectations library although I think dbt has its own built-in solution for this
- CICD enforced via:
    - branch protection requiring at least one peer review
    - pre-commit hooks to ensure standardisation of code formatting
    - running pytest to perform tests on functions in growthleads_etl module
    - (dbt now also has unit test funcitonality - seperate from its current data tests )
    - All of this managed by GitActions upon push
- Having data_sources configured as `"load_type": "append",`, seems to cause a race condition on the inital run of web_traffic_dag. This is an issue with the to_sql() in load_dataframe_to_postgres() as when using pandas.DataFrame.to_sql() with if_exists='append' on a database that doesn't yet have the target table, it first attempts to create the table using SQLAlchemy. If two tasks or connections try to create the same table concurrently, you encounter the psycopg2.errors.UniqueViolation error because both tasks are attempting to create the same table structure simultaneously. I wanted to avoid having a `CREATE TABLE` procedure before any loading to keep it flexible. Future dev would look to dynamically generate a `CREATE TABLE` script based off of a pandera schema defined in growthleads_etl/schemas.py  
    - The current workaround is to have task retry set to 3 seconds,as this task always works on the 2nd attempt
