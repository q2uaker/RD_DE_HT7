from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

import sys
sys.path.append('./HT7')
import json
import os
from HT7.config import Config
from HT7.complex_http_operator import ComplexHttpOperator


config = Config(os.path.join('/','home','user','airflow','dags','HT7', 'config.yaml'))
config = config.get_config('HT1_app')

dag = DAG(
    dag_id="HT7_dag",
    description="Hometask of 7th lesson",
    start_date = datetime(2021,6,19,12,00),
    schedule_interval = '@daily',
    user_defined_macros={
        'json': json
        }
    )

           
login_task = SimpleHttpOperator(
                       task_id = "robodreams_api_login",
                       dag = dag,
                       http_conn_id="rd_api",
                       headers = {"content-type": "application/json"},
                       method="POST",
                       endpoint= "auth",
                       data = json.dumps({"username": config['username'], "password": config['password']}),
                       xcom_push=True
                       )

load_task = ComplexHttpOperator(
                       task_id = "robodreams_api",
                       dag = dag,
                       http_conn_id="rd_api",
                       method="GET",
                       headers = {'content-type': 'text/plain',
                                  'Authorization': 'JWT ' + '{{json.loads(ti.xcom_pull(task_ids="robodreams_api_login", key="return_value"))["access_token"]}}',
                                  },
                       endpoint=config['data_point'],
                       data = {"date": str("{{ds}}")},
                       save_on_disk=True
                       )
login_task >> load_task