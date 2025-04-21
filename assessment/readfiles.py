from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import broadcast
from pyspark import StorageLevel
from pyspark.sql.types import *


from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import  PythonOperator



def func(file_path1,file_path2):

    spark = SparkSession.builder.appName("assessment").getOrCreate()
    x_df = spark.read.csv(file_path1,header =True,inferSchema = True)

    y_df = spark.read.csv(file_path2,header =True,inferSchema =True)



    return x_df , y_df