# Simple ETL Airflow
* This is a project of **Data Warehouse subject** in my university. 
* In this project we have to design and implement a data warehouse (postgreSQL) from an existing [mySQL mmis database](\20220205_mmis.sql), which contains inventory data.

## Data Warehouse design
The design is implemented in the script [psql-dwh.sql](\psql-dwh.sql)
* This is just a simple Star Schema to store data from mySQL database
* This design have some problems to work on real-life context such as: slowly change dimensions, storing changed/deleted history records, ...

## Airflow ETL description
Our ETL only run once using manual trigger
* The data is transformed using pandas Dataframe
* We depends on a tables named ***inventory_etl_manager.database_flow_reference_table*** which is implemented in [initialize_reference_table.py](\initialize_reference_table.py)

## Installation
* Step 1: Clone this repository 
* Step 2: Create your python virtual enviroment
* Step 3: Install python packages 
```
pip install pandas psycopg2 numpy mysql-connector-python datetime
```
* Step 4: Set up airflow directory, YOU HAVE TO RUN THIS COMMAND ON EVERY TERMINAL BEFORE RUNNING AIRFLOW or you can find a way to export this into your zsh_src or bash_src
```
export AIRFLOW_HOME=.
```
* Step 5: Install Airflow 2.2.3, you can read [docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) for more information
```
pip install "apache-airflow[celery]==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-${PYTHON_VERSION}.txt"
```
* Step 6: Init database and create user
```
airflow db init
airflow users create \
          --username admin \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org
```
* Step 7: Start the server
```
airflow webserver -p 8080
```
* Step 8: Start the scheduler
```
airflow scheduler
```

## Trigger ETL
* Modify the database information in [etl-in-full.py](\dags\etl-in-full.py)
* Run the script [initialize_reference_table.py](\initialize_reference_table.py) to implement the etl-mamangement table
* Go to the etl_mysql_to_postgresql dag in your screen and clicked play button in the top right corner of the screen
    * The dag is written on [etl-in-full.py](\dags\etl-in-full.py)
    * The warehouse is set to be reset everytime the dag is triggered