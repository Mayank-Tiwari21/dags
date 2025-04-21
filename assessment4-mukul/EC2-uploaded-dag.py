from airflow import DAG
from datetime import datetime
import sys
import boto3
import os
import time
#libraries for the amazon services
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
#libraries for the airflow operators
from airflow.operators.python import ShortCircuitOperator, PythonOperator

#defining the global variables to hold the paths.









#adding the scripts to import the functions from'
#sys.path.insert(0,'/home/ec2-user/airflow/dags/uploader_function2.py')
#from uploader_function2 import upload_to_s3 , files_checker

#python functions



def run_existing_crawler():
    crawler_name = 'poc-bootcamp-mayank-assessment4-dataset1'
    glue = boto3.client('glue', region_name='us-east-1')

    # Start the crawler
    glue.start_crawler(Name=crawler_name)
    print(f"Crawler '{crawler_name}' started.")

    # Wait for completion
    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']
        print(f"Current state: {state}")
        if state == 'READY':
            print(f"Crawler '{crawler_name}' has completed.")
            break
        time.sleep(10)






def files_checker():
    return len(os.listdir("/home/ec2-user/data_files/"))>0



def upload_to_s3():
    s3 = boto3.client('s3')
    bucket_name = "ttn-de-bootcamp-2025-silver-us-east-1"

    # Define datasets
    datasets = {
        "dataset1": "mayank/assessment4/data/bronze/dataset1",
        "dataset2": "mayank/assessment4/data/bronze/dataset2"
    }

    for dataset, s3_folder_prefix in datasets.items():
        local_path = os.path.expanduser(f"~/data_files/{dataset}/")

        if os.path.exists(local_path) and os.path.isdir(local_path):
            files = os.listdir(local_path)

            if files:
                for file_name in files:
                    file_path = os.path.join(local_path, file_name)

                    if os.path.isfile(file_path):
                        s3_key = os.path.join(s3_folder_prefix, os.path.basename(file_name))

                        try:
                            s3.upload_file(file_path, bucket_name, s3_key)
                            print(f"Uploaded: {file_name} --> s3://{bucket_name}/{s3_key}")
                            os.remove(file_path)
                        except Exception as e:
                            print(f"Failed to upload {file_name}: {e}")
                print(f"All files processed in {dataset}")
            else:
                print(f"No files found in {dataset}")
        else:
            print(f"Directory does not exist for {dataset}: {local_path}")


#defining the dag
default_args = {
    'owner':'mayank'
}

with DAG('airflow_orchestrated_dag',
         schedule_interval='@hourly',
         start_date= datetime(2025,4,11),
          default_args= default_args,
         catchup=False ) as assessment_dag :
    task1_fileChecker = ShortCircuitOperator(
        task_id = "file_check",
        python_callable=files_checker
    )
    task2_uploadToS3 = PythonOperator(
        task_id = 'uploadToS3',
        python_callable=upload_to_s3
    )

    task3_lambdaInvoke  = LambdaInvokeFunctionOperator(
        task_id = 'invokeTheLambdaFunction',
        function_name = 'poc-bootcamp-mayank-assessment4',
        log_type = 'Tail',
        invocation_type= 'RequestResponse',
        aws_conn_id='aws_default'
    )

#    task4_glueCrawler = GlueCrawlerOperator(
#        task_id = "CreateTableThroughCrawler",
#        crawler_name = 'poc-bootcamp-mayank-assessment4-dataset1',
#        wait_for_completion='True'
#    )
    task4_glueCrawler = PythonOperator(
        task_id = "CreateTableThroughCrawler",
        python_callable = run_existing_crawler
       
    )
    task5_glueJob = GlueJobOperator(
        task_id = 'ExecuteTheGlueJob',
        job_name = 'poc-bootcamp-mayank-assessment4',
        wait_for_completion= True,
        aws_conn_id='aws_default'
    )
    task6_athenaQuery1 = AthenaOperator(
        task_id = "athenaQueryOnCovid_19_India",
        query = '''                   
                select t1.date,state  from crawled_dataset2 as t1
                join ( select date,max(positive/totalsamples*100) as positive_rate
                        from crawled_dataset2
                        group by date) as t2 
                on t1.date = t2.date and (t1.positive/t1.totalsamples*100)=t2.positive_rate
                order by t1.date asc;''',
        database= "poc-bootcamp-mayank_covid_india_data",
        aws_conn_id='aws_default'
    )
    task7_athenaQuery2 = AthenaOperator(
        task_id = "athenaQueryOnStatewiseTestingDetails",
        query = '''
                select *,confirmedforeignnational/(select sum(confirmedforeignnational+confirmedindiannational)  from crawled_dataset1) as confirmedforeignnational_per
                from crawled_dataset1;''',
        database = "poc-bootcamp-mayank_covid_india_data",
        aws_conn_id='aws_default'
    )
    task8_EMRJob = EmrAddStepsOperator(
        task_id = "EMRClusterTask",
        job_flow_id = "<cluster_id>",
        steps=[{
            'Name':'SparkJobOnEmr',
            'ActionOnFailure':'CONTINUE',
            'HadoopJarStep':{
                'Jar':'command-runner.jar',
                'Args':[
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.driver.cores=1',
                '--conf', 'spark.driver.memory=2g',
                '--conf', 'spark.executor.cores=1',
                '--conf', 'spark.executor.memory=2g',
                's3://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/code-EMR/spark_job.py'
                ]
            }
        }],
        aws_conn_id='aws_default'
    )
    #dag task execution structure
    task1_fileChecker>>task2_uploadToS3>>task3_lambdaInvoke>>task4_glueCrawler>>task5_glueJob>> [task6_athenaQuery1 , task7_athenaQuery2] >>task8_EMRJob

