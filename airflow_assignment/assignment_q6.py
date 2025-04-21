#from internal lib
from datetime import datetime

#from airflow lib
from  airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#python functions
def _task3_func(**kwargs):
    counter:int = 1
    print(f"this is the {counter} py_prog")
    kwargs['ti'].xcom_push(key = 'raw_data',value = counter)

def _task5_func(**kwargs):
    counter = kwargs['ti'].xcom_pull(task_ids ="1st_python_operator",key = 'raw_data' )
    counter+=1
    kwargs['ti'].xcom_push(key = 'raw_data',value = counter)
    print(f"this is the {counter} py_prog")

def _task6_func(**kwargs):
    counter = kwargs['ti'].xcom_pull(task_ids = '2nd_python_operator',key ='raw_data')
    counter+=2
    kwargs['ti'].xcom.push(key='raw_data',value=counter)
    print(f"This is the {counter} py_prog")


#Default_args for defining the owner of the dag
Default_args = {
    'owner':'mayank'
}

#main dag
with DAG('my_assignment_q6',
         schedule_interval= '@daily',
         default_args= Default_args,
         start_date=datetime(2025,3,8),
         catchup=False) as my_dag_assignment_q6:
    
    #bash task1
    task_1 = EmptyOperator(
        task_id = "start"
    )

    #group task1
    with TaskGroup("group_t1") as t1:
        task_2  = BashOperator(
            task_id = "bash_operation1",
            bash_command= 'echo "This is the output of a bash operator"'
        )
        task_3 = PythonOperator(
            task_id = '1st_python_operator',
            python_callable=_task3_func
        )
        task_2>>task_3
    
    #group task2
    with TaskGroup("group_t2") as t2:
        task_4 = BashOperator(
            task_id = 'bash_operation2',
            bash_command='echo "This is the output of teh second bash operator"'
        )
        task_5 = PythonOperator(
            task_id = "2nd_python_operator",
            python_callable = _task5_func
        )
        task_4>>task_5
    #group task3
    with TaskGroup("group_t3") as t3:
        task_6 = PythonOperator(
            task_id = "3rd_python_operator",
            python_callable = _task6_func
        )
        task_6
    task_1>>t1>>t2>>t3
    