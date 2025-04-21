from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner':'mayank'
}



with DAG("my_dag2_bash",
        default_args = default_args,
        start_date = datetime(2025,3,4),
        schedule_interval = '10 * * * *'
        ) as the_dag_bash:


        task1 = BashOperator(task_id='find_the_user',bash_command = 'whoami')
        task2 = BashOperator(task_id='echoing',bash_command='echo "Hello user"')

        task1>>task2