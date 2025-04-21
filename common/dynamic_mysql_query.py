#for system conversion of date and time
from datetime import datetime



#for the airflow functionality
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.operators.mysql  import MySqlOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

Default_args = {
    'owner':'mayank'
}
#python tasks


#DAG layout
with DAG(
    "my_dynamic_mysql_query",
    default_args = Default_args,
    start_date = datetime(2025,4,1),
    schedule_interval = '10 04 * * *',

) as dynmo_sql:
    
    #opening the sql script
    with open('/home/mayank/airflow/dags/sql_scripts/dynamic_mysql_query/select_all.sql','r') as sql_script:
        select_all = sql_script.read()
    sql_script.close()

    #task 1 to read all the scripts     
    task_1 = SQLExecuteQueryOperator(
        task_id = "first_task",
        conn_id = 'my_sql_connection',
        sql = select_all
    )

    #task 2 to read specifically read the finance dept data only
    with TaskGroup("finance_task") as task_on_finance:

        #opening file and reading the script 
        with open('/home/mayank/airflow/dags/sql_scripts/dynamic_mysql_query/select_all_finance.sql','r') as sql_script:
            select_all_finance = sql_script.read()
        sql_script.close()

        #subtask for the group finance_task
        task_2a = SQLExecuteQueryOperator(
            task_id = 'select_all_finance',
            conn_id = 'my_sql_connection',
            sql = select_all_finance
        )

        #opening the file to count the data 
        with open('/home/mayank/airflow/dags/sql_scripts/dynamic_mysql_query/count_finance.sql','r') as sql_script:
            count_all_finance =sql_script.read()
        sql_script.close()

        #subtask to count the no.of employees in teh finance department
        task_2b = SQLExecuteQueryOperator(
            task_id = 'count_all_finance',
            conn_id = 'my_sql_connection',
            sql = count_all_finance,
        )

        task_2a >> task_2b
    task_1 >> task_on_finance
