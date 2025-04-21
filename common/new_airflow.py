from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator



from random import randint
def new_read_func(**kwargs):
    postgres = PostgresHook(postgres_conn_id = "my_postgres_connection")
    conn = postgres.get_conn()
    cursor =conn.cursor()
    cursor.execute("select *  from tt;")
    result = cursor.fetchall()

    for row in result:
        print(row)
    
    kwargs["ti"].xcom_push(key ="raw",value = result)
    cursor.close()
    conn.close()

def filter_data(**kwargs):
    result = kwargs["ti"].xcom_pull(task_ids = "<task_id>",key = "raw")
    filtered_data = [row for row in result if row[2]=="Finance"]

    for row in filtered_data:
        print(row)
def brancher(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids = ["thirdPythontask","fourthPythonTask"])
    max_data = sorted(data)
    if max_data>5:
        return "new_grt"
    return "old_grt"

def random_val():
    r = randint(0,10)
    return r 





with DAG("my_dag",
         default_args=Default,
         start_date=datetime(2025,3,12),
         catchup=True,
         schedule_interval="10 * 21 03 *") as my_dag:
    
    task1 = PythonOperator(task_id= "myfirstPythonTask",
                           python_callable=new_read_func)
    task2 = PythonOperator(task_id = "mysecondPythonTask",
                           python_callable = new_read_func)
    
    with TaskGroup(group_id="grouptask") as tasking_grp:
        task_g1 = PostgresOperator(task_id = "firstpostgrestask",
                                   postgres_conn_id="my_posgres_connection",
                                   sql = "select * from tt;")
        taskg2 = SQLExecuteQueryOperator(task_id ="sqlTaskgrp",
                                         conn_id="my_sql_connection",
                                         sql="select * from tt;")
        task_g1>>taskg2
    task_3 = PythonOperator(task_id = "thirdPythontask",
                            python_callable=random_val)
    task_4 = PythonOperator(task_id = "fourthPythonTask",
                            python_callable=random_val)
    task_dec = BranchPythonOperator(task_id = "branch",
                                    python_callable=brancher)
    
    task_5  =BashOperator(task_id = "firstBashTAsk",
                          bash_command='echo "Hello"')
    task_6 = BashOperator(task_ids = "secondBashTask",
                          bash_command='echo "heh"')
    task1>>task2>>tasking_grp>>[task_3,task_4]>>task_dec>>[task_5,task_6]