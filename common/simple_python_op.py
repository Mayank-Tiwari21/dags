from datetime import datetime


#importing airflow libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.operators.bash import BashOperator


#pyhton functions
def reader_code(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from tt;")
    rows = cursor.fetchall()
    count: int = 0
    with open("/home/mayank/airflow/dags/csv_files/simple_python_op/tt.csv",'a') as f:
        for row in rows:
            f.write(','.join(str(cell)for cell in row)+'\n')
            count +=1
    kwargs['ti'].xcom_push(key='countable',value = count)

def counter_func (**kwargs):
    counter = kwargs['ti'].xcom_pull(task_ids = 'read_data',key = 'countable')
    print(counter)

#defining the dag owner
Default_args = {
    'owner':'mayank'
}
with DAG ("simply_py_op_dag",
          start_date= datetime(2025,3,7),
          schedule_interval= None,
          default_args= Default_args) as simply_py_dag:
    task_1 = PythonOperator(
        task_id = "read_data",
        python_callable=reader_code
    )

    task_2 = PythonOperator(
        task_id = 'count_the_number_of_entries',
        python_callable=counter_func
    )
    
    task_1>> task_2
