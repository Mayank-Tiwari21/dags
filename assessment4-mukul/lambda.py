# import json
# import boto3
# from pyspark.sql import SparkSession
# from pysaprk.sql.functions import col
# import os
# from io import BytesIO

# s3 = boto3.client('s3')
# bucket_name = 'ttn-de-bootcamp-2025-silver-us-east-1'

# bronze_folder = 'mayank/assessment4/data/bronze/'
# silver_folder = 'mayank/assessment4/data/silver/lambda/'

# processed_files_path = 'mayank/assessment/data/silver/processesd_files.txt'

# spark = SparkSession.builder.appName("DataFixer").getOrCreate()

# #function to read the processed data
# def read_processed_files():
#     try:
#         response = s3.get_object(Bucket = bucket_name, Key = processed_files_path)
#         content = response['Body'].read().decode("utf-8").splitlines()
#         return content
#     except s3.exceptions.NoSuchKey:
#         return[]
    
# #function to write the names of the proessed files to teh designated destination

# def write_processed_files (processed_files):
#     try:
#         s3.pu_object(Body = '\n'.join(processed_files),Bucket = bucket_name, Key = processed_files_path)
#         print(f"successfully updated the {processed_files_path}")
#     except NoCredentialError:
#         print("AWS credentials not found")


# def event_handler(event,context):
#     processed_files = read_processed_files()

#     response = s3.list_objects_v2(Bucket = bucket_name, Prefix = bronze_folder)
#     files_processed = [content['Key'] for content in response.get('Contents',[])]


#     for file_key in files_processed:
#         file_name = file_key.split('/')[-1]
#         if file_name not in processed_files:
#             file_obj = s3..get_object(Bucket = bucket_name, Key = file_key)
#             file_content = file_obj['Body'].read()

#             df = spark.read.csv(BytesIO(file_content),header = True,inferSchema =True)

#             for column in df.columns:
#                 if dict(df.dtypes)[column] in ['int','double']:
#                     df = df.fillna({column:0})
#             silver_key = f"{silver_folder}{file_name}"

#             try:
#                 df.write.csv(f"s3://{bucket_name}/{silver_key}",header = True,mode = 'append')
#                 print(f"successfully saved {file_name} to {silver_key}")
#                 processed_files.append(file_name)
#             except NoCredentialError:
#                 print("AWS creds not found")
#     write_processed_files(processed_files)



# import json
# import boto3
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import os
# from io import BytesIO
# from botocore.exceptions import NoCredentialsError

# s3 = boto3.client('s3')
# bucket_name = 'ttn-de-bootcamp-2025-silver-us-east-1'

# bronze_base_path = 'mayank/assessment4/data/bronze/'
# silver_base_path = 'mayank/assessment4/data/silver/lambda/'

# processed_files_path = 'mayank/assessment4/data/silver/processed_files.txt'

# spark = SparkSession.builder.appName("DataFixer").getOrCreate()

# # Function to read processed files from S3
# def read_processed_files():
#     try:
#         response = s3.get_object(Bucket=bucket_name, Key=processed_files_path)
#         content = response['Body'].read().decode("utf-8").splitlines()
#         return content
#     except s3.exceptions.NoSuchKey:
#         return []

# # Function to write processed files list back to S3
# def write_processed_files(processed_files):
#     try:
#         s3.put_object(Body='\n'.join(processed_files), Bucket=bucket_name, Key=processed_files_path)
#         print(f"Successfully updated processed files in {processed_files_path}")
#     except NoCredentialsError:
#         print("AWS credentials not found")

# # Main Lambda handler
# def event_handler(event, context):
#     processed_files = read_processed_files()

#     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bronze_base_path)
#     all_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

#     for file_key in all_files:
#         if file_key in processed_files:
#             continue

#         file_name = file_key.split('/')[-1]
#         subfolder = file_key.split('/')[4]  # Extract 'dataset1' or 'dataset2' from the path

#         # Read the file
#         file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
#         file_content = file_obj['Body'].read()

#         # Load CSV into Spark DataFrame
#         df = spark.read.csv(BytesIO(file_content), header=True, inferSchema=True)

#         # Fill missing numeric values with 0
#         for column in df.columns:
#             if dict(df.dtypes)[column] in ['int', 'double']:
#                 df = df.fillna({column: 0})

#         # Define output path
#         silver_key = f"{silver_base_path}{subfolder}/{file_name}"

#         try:
#             df.write.csv(f"s3://{bucket_name}/{silver_key}", header=True, mode='overwrite')
#             print(f"Successfully saved {file_name} to {silver_key}")
#             processed_files.append(file_key)  # Save the full key to track uniquely
#         except NoCredentialsError:
#             print("AWS credentials not found")

#     # Update processed files list
#     write_processed_files(processed_files)





import json
import boto3
import pandas as pd
import os
from io import BytesIO, StringIO
from botocore.exceptions import NoCredentialsError

s3 = boto3.client('s3')
bucket_name = 'ttn-de-bootcamp-2025-silver-us-east-1'

bronze_base_path = 'mayank/assessment4/data/bronze/'
silver_base_path = 'mayank/assessment4/data/silver/lambda/'

processed_files_path = 'mayank/assessment4/data/silver/processed_files.txt'

# Function to read processed files from S3
def read_processed_files():
    try:
        response = s3.get_object(Bucket=bucket_name, Key=processed_files_path)
        content = response['Body'].read().decode("utf-8").splitlines()
        return content
    except s3.exceptions.NoSuchKey:
        return []

# Function to write processed files list back to S3
def write_processed_files(processed_files):
    try:
        s3.put_object(Body='\n'.join(processed_files), Bucket=bucket_name, Key=processed_files_path)
        print(f"Successfully updated processed files in {processed_files_path}")
    except NoCredentialsError:
        print("AWS credentials not found")

# Main Lambda handler
def lambda_handler(event, context):
    processed_files = read_processed_files()

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bronze_base_path)
    all_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

    for file_key in all_files:
        if file_key in processed_files:
            continue

        file_name = file_key.split('/')[-1]
        subfolder = file_key.split('/')[4]  # Extract 'dataset1' or 'dataset2'

        # Read the file
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')

        # Load CSV into pandas DataFrame
        df = pd.read_csv(StringIO(file_content))

        # Fill missing numeric values with 0
        for column in df.select_dtypes(include=['int64', 'float64']).columns:
            df[column] = df[column].fillna(0)

        # Convert DataFrame back to CSV (as a string)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Define output path
        silver_key = f"{silver_base_path}{subfolder}/{file_name}"

        try:
            # Upload the CSV string to S3
            s3.put_object(Bucket=bucket_name, Key=silver_key, Body=csv_buffer.getvalue())
            print(f"Successfully saved {file_name} to {silver_key}")
            processed_files.append(file_key)
        except NoCredentialsError:
            print("AWS credentials not found")

    # Update processed files list
    write_processed_files(processed_files)
