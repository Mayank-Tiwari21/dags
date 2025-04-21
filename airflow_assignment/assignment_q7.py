#from the system
from datetime import datetime
from random import randint
#from the airflow
from airflow import DAG
from airflow.operators.python import PythonOperator , ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

#python functions
def random_val_generator(**kwargs):
    rand = randint(0,10)
    kwargs['ti'].xcom_push(key = 'random_number',value = rand)
    print(rand)


def python_logic(**kwargs):
    rand = kwargs['ti'].xcom_pull(task_ids = 'first_task',key = "random_number")
    if rand>8:
        return True
    return False


#default_args
Default_args = {
    'owner':'mayank'
}

#my dag
with DAG("my_assignment_q7",
         default_args=Default_args,
         schedule_interval= '@daily',
         start_date=datetime(2025,3,8)
         ) as my_dag_q7:
    
    #first_task
    task_1 = PythonOperator(
        task_id = "fist_task",
        python_callable= random_val_generator
    )

    task_2 = ShortCircuitOperator(
        task_id= "short_circuit_operator",
        python_callable = python_logic
    )

    task_3 = BashOperator(
        task_id = 'task_3_bash',
        bash_command='echo "This is the third task"'
    )

    task_1>> task_2>>task_3