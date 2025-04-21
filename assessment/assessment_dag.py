from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import  PythonOperator


#python




with DAG("assessment_dag",
         start_date=datetime(2025,3,26),
         schedule_interval="30 * 1 * *") as assess_dag:
    task1 = SparkSubmitOperator(
        task_id = "csv_first_read",
        application="/home/hdoop/assessment/readfiles.csv",
        files=""
    )
    task2 = SparkSubmitOperator(
        task_id = "csv_first_read",
        application="/home/hdoop/assessment/readfiles.csv",
        files=""
    )

    task1>>task2