import os
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from helpers.connections import Mysql, Postgresql

from transform import *

from airflow.decorators import dag, task
from airflow.utils import dates

from password import *

CUR_DIR = os.path.abspath(os.getcwd())
POSTGRE_HOST, POSTGRE_PORT, POSTGRE_DB_NAME, POSTGRE_USER, POSTGRE_PASSWORD = 'localhost', '5432', 'postgres', 'postgres', ''
MYSQL_HOST, MYSQL_PORT, MYSQL_DB_NAME, MYSQL_USER, MYSQL_PASSWORD = 'localhost', '3306', 'mmis', 'root', psql_password

def get_reference_table() -> DataFrame:
    """
    Description: Downloads reference table from PostgreSQL database.
    """
    ETL_MANAGER_DB = 'inventory_etl_manager'
    conn_obj = Postgresql(host=POSTGRE_HOST, port=POSTGRE_PORT, db_name=POSTGRE_DB_NAME, user_name=POSTGRE_USER, password=POSTGRE_PASSWORD)
    
    query = f"""SELECT * FROM {ETL_MANAGER_DB}.database_flow_reference_table"""
    ref_table = conn_obj.execute_query(query=query, return_data=True)
    conn_obj.close_connection()  # close connection to greenplum db
    return ref_table

ref_table = get_reference_table()

default_args = {
    'owner': 'DWH_Group2',
    'start_date': dates.days_ago(1),
    'retries': 1
}

@dag(dag_id = 'etl_mysql_to_postgresql', default_args=default_args, schedule_interval=None, tags=['mySQL', 'pSQL', 'ETL'])
def etl():
    @task()
    def extract():
        mysql = Mysql(host=MYSQL_HOST, port=MYSQL_PORT, db_name=MYSQL_DB_NAME, user_name=MYSQL_USER, password=MYSQL_PASSWORD)
        dfs = {}

        #print (ref_table)
        #print(ref_table['key_fields'].values[0])
        #print (f"select {ref_table['key_fields']} from {ref_table['source_schema']}.{ref_table['source_table']};")

        for idx, row in ref_table.iterrows():
            source_schema = row['source_schema']
            source_table = row['source_table']
            key_fields = row['key_fields']
            #query = f"select city, country, birthdate, yearly_income, gender, total_children, education, occupation from mmis.customer;"
            query = f"select {key_fields} from {source_schema}.{source_table};"
            data_df = mysql.execute_query(query=query, return_data=True)

            file_name = f"{source_schema}_{source_table}.csv"
            file_path = CUR_DIR + '/temp_data/' + file_name
            data_df.to_csv(file_path, index=False)

            dfs[source_table] = file_path

        return dfs
    
    @task()
    def transform(dfs):
        transformed_dfs = {}
        transformed_dfs['customers'] = transform_customer(data_df=pd.read_csv(dfs['customer']))
        transformed_dfs['products'] = transform_product(pd.read_csv(dfs['product']), pd.read_csv(dfs['product_class']))
        transformed_dfs['promotions'] = transform_promotion(pd.read_csv(dfs['promotion']))
        transformed_dfs['stores'] = transform_store(pd.read_csv(dfs['store']))
        transformed_dfs['warehouses'] = transform_warehouse(pd.read_csv(dfs['warehouse']))
        transformed_dfs['time_by_day'] = transform_time(pd.read_csv(dfs['time_by_day']))
        transformed_dfs['inventory_fact'] = transform_concat_inventory(dfs, transformed_dfs['promotions'])

        # transformed_dfs['inventory_fact'] = transform_inventory(dfs['inventory_fact_1997'], dfs['time_by_day'], transformed_dfs['promotions'])
        # transformed_dfs['inventory_fact'] = pd.concat([transformed_dfs['inventory_fact'], transform_inventory(dfs['inventory_fact_1998'], dfs['time_by_day'], transformed_dfs['promotions'])], axis=0)
        return transformed_dfs
    
    @task()
    def load (processed_dfs):
        psql = Postgresql(host=POSTGRE_HOST, port=POSTGRE_PORT, db_name=POSTGRE_DB_NAME, user_name=POSTGRE_USER, password=POSTGRE_PASSWORD)

        sql_file = open("psql-dwh.sql")
        sql_as_string = sql_file.read()
        psql.execute_query(sql_as_string, False)
        
        for idx, row in ref_table.iterrows():
            #get the destination reference
            destination_schema = row['destination_schema']
            destination_table = row['destination_table']
            target_fields = row['target_fields']

            if destination_table:
                df = pd.read_csv(processed_dfs[destination_table])
                psql.insert_values(df, destination_schema, destination_table, target_fields)

    # [START main_flow]
    dfs = extract()
    processed_dfs = transform(dfs)
    load(processed_dfs)
    # [END main_flow]

# [START dag_invocation]
etl_dag = etl()
# [END dag_invocation]