from airflow import DAG
from datetime import datetime
import sys
import boto3
#libraries for the amazon services
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator 
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
#libraries for the airflow operators
from airflow.operators.python import ShortCircuitOperator, PythonOperator

#defining the global variables to hold the paths.

def run_existing_crawler():
    crawler_name = 'poc-bootcamp-mayank-assessment4-dataset1'
    glue = boto3.client('glue')

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
        time.sleep(10)  # wait before checking again


#adding the scripts to import the functions from'
# sys.path.insert(0,'/home/mayank/airflow/dags/assessment4-mukul/uploader_function2.py')
from uploader_function2 import upload_to_s3 , files_checker



#defining the dag
default_args = {
    'owner':'mayank'
}

with DAG('airflow_orchestrated_dag',
         schedule_interval='@hourly',
         start_date= datetime(2025,4,11),
          default_args= default_args ,
          catchup=False) as assessment_dag :
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

        

    )

    task4_glueCrawler = GlueCrawlerOperator(
        task_id = "CreateTableThroughCrawler",
        cawler_name = 'poc-bootcamp-mayank-assessment4-dataset1',
        wait_for_completion='True'
    )
    task5_glueJob = GlueJobOperator(
        task_id = 'ExecuteTheGlueJob',
        job_name = 'poc-bootcamp-mayank-assessment4',
        wait_for_completion= True
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
        database= "poc-bootcamp-mayank_covid_india_data"

    )
    task7_athenaQuery2 = AthenaOperator(
        task_id = "athenaQueryOnStatewiseTestingDetails",
        query = '''
                select *,confirmedforeignnational/(select sum(confirmedforeignnational+confirmedindiannational)  from crawled_dataset1) as confirmedforeignnational_per
                from crawled_dataset1;''',
        database = "poc-bootcamp-mayank_covid_india_data"
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
        }]
    )
    #dag task execution structure
    task1_fileChecker>>task2_uploadToS3>>task3_lambdaInvoke>>task3_lambdaInvoke>>task4_glueCrawler>>task5_glueJob>> [task6_athenaQuery1 , task7_athenaQuery2] >>task8_EMRJob

