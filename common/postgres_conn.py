from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator

#func for fetching the data
def fetch_data_from_postgres(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')
    conn =postgres_hook.get_conn()
    cursor = conn.cursor()

    #fetch all the data using select * 
    cursor.execute('select * from tt;')
    rows=cursor.fetchall()

    #print teh data
    for row in rows:
        print(row)

    #push dat to XCom for the next task
    kwargs['ti'].xcom_push(key='raw_data',value=rows)

    cursor.close()
    conn.close()

#func to filter the data based on the condition
def filter_data(**kwargs):

    #retrieve data from the xcom
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data_task',key ='raw_data')

    #filter rows where dept = finance
    filtered_data = [row for row in raw_data if row[2]=='Finance']

    print("filtered data (dept = 'Finance')")
    for row in filtered_data:
        print(row)

default_args = {
    'owner':'mayank'
    
}


dag_postgresql = DAG(dag_id = "postgres_dag",
    schedule_interval = '10 18 * * *',
    default_args = default_args,
    start_date=datetime(2025,3,5)

) 

#task 1: feth data
fetch_data_task = PythonOperator(
    task_id = 'fetch_data_task',
    python_callable = fetch_data_from_postgres,
    provide_context =True,
    dag = dag_postgresql
)

#task 2: filter data
filter_data_task = PythonOperator(
    task_id = 'filter_data_task',
    python_callable = filter_data,
    provide_context = True,
    dag = dag_postgresql
)



#define dependencies
fetch_data_task >> filter_data_task
