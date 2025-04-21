#from python library
from datetime import datetime
import json


#from airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


#vriable definition
file_path = '/home/mayank/airflow/dags/airflow_assignment/json_files/players.json'

#python functions
def read_file_path(**kwargs):
    with open(file_path ,'r') as file:
        data =json.load(file)
    total_runs =sum(players['run'] for players in data.values())
    kwargs['ti'].xcom_push(key = 'total_runs',value= total_runs)
    

def print_total_runs(**kwargs):
    total_runs =kwargs['ti'].xcom_pull(task_ids='read_file_path',key='tota_runs')
    print(total_runs)
#default_arguments
Default_args = {
    'owner':'mayank'
}


#my_dag
with DAG ("my_assignment_q8",
          default_args =Default_args,
          schedule_interval='0 0 * * *',
          start_date = datetime(2025,3,8)
          ) as my_dag :
    

    task_1 = PythonOperator(
        task_id = "read_file_path",
        python_callable = read_file_path
    )

    task_2 =PythonOperator(
        task_id = 'print_total_runs',
        python_callable=print_total_runs
    )
    task_1>>task_2