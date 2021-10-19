from datetime import datetime
from airflow import DAG



import sys
sys.path.append('./HT7')
import json
import os

from HT7.my_postgres_operators import myPostgresOperator_ListToCSV
from HT7.my_postgres_operators import myPostgresOperator_TablesToCSV


dag = DAG(
    dag_id="HT7_dag_db",
    description="Hometask of 7th lesson DB Load",
    start_date = datetime(2021,10,19,12,00),
    schedule_interval = '@once',
    )

           
Get_all_tables = myPostgresOperator_ListToCSV(
                       task_id = "load_db_tables",
                       dag = dag,
                       postgres_conn_id="Postgres_HT7",
                       database="dshop",
                       folder=os.path.join('/','home','user','HT7_download'),
                       filename='tables.csv',
                       sql = """
                       SELECT table_name
                       FROM information_schema.tables
                       WHERE table_schema='public'
                       """
                       )

Save_tables = myPostgresOperator_TablesToCSV(
                       task_id = "save_tables",
                       dag = dag,
                       postgres_conn_id="Postgres_HT7",
                       database="dshop",
                       folder=os.path.join('/','home','user','HT7_download'),
                       tables_csv_filename=os.path.join('/','home','user','HT7_download','tables.csv'),
                       sql=""
                       )

Get_all_tables >> Save_tables