# Growthleads Tech Task

## Prerequisites
 - Docker

## Setup
 - clone this repo and cd into:  
 ```
 git clone https://github.com/LouisYC123/growthleads-tech-task.git
 cd growthleads-tech-task
 ```
- add the following .env file to /docker
    ```
        POSTGRES_USER=<choose_a_username>
        POSTGRES_PASSWORD=<choose_a_password>
        PGADMIN_LISTEN_PORT=5050  
        DATA_DIR=<directory_to_host_data_landing_zone>
    ```
- build the image for the AWS MWAA local runner:  
    ```./mwaa-local-env build-image```
- Open the Airflow UI in a browser at:  
    ``` localhost:8080/home ```
- Login using 'admin' and 'test' credentials
- Head to Admin -> connections and add the following connection:  
    ```
    connection_id: postgres_conn_id
    host: pg_container
    database: growthleads
    login:  <the username you chose in the above .env file>
    password: <the password you chose in the above .env file>
    port: 5432
    ```

## Usage
 - You can add or configure data sources in ```plugins/growthleads_etl/data_sources```


## Usage
 - if you want to run dbt outside of airflow, you have to update 'host' in the dbt/profiles.yml to = 'localhost'