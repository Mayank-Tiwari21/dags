import boto3
import os
def upload_to_s3():
    #creating a given s3 type client 
    s3 = boto3.client('s3')
    bucket_name = "ttn-de-bootcamp-2025-silver-us-east-1"
    s3_folder_prefix1 = "mayank/assessment4/data/bronze/dataset1"
    s3_folder_prefix2 = "mayank/assessment4/data/bronze/dataset2"
    #local path for the files 
    path_to_local1 = os.path.expanduser("~/data_files/dataset1/")
    path_to_local2 = os.path.expanduser("~/data_files/dataset2")
   
    #checking if the folder exists at the path for dataset 1
    if os.path.exists(path_to_local1) and os.path.isdir(path_to_local1):
        #listing the files in the given folder
        files = os.listdir(path_to_local1)
        
        #if the files are present
        if files:
            for file_name in files:
                #creatin the path to the files
                file_path = os.path.join(path_to_local1,file_name)

                #if the founded name belongs to a given file
                if os.path.isfile(file_path):
                    s3_key = os.path.join(s3_folder_prefix1,file_name)
                    s3.upload_file(file_path,bucket_name,s3_key)
                    print(f"uploaded the {file_name}-->s3://{bucket_name}/{s3_key}")
                    os.remove(file_path)
            print("all the files deleted")
        else:
            print("no files to delete")
    else:
        print("No such folder exists")

    #for dataset 2 
    if os.path.exists(path_to_local2) and os.path.isdir(path_to_local2):
        #listing the files in the given folder
        files = os.listdir(path_to_local2)
        
        #if the files are present
        if files:
            for file_name in files:
                #creatin the path to the files
                file_path = os.path.join(path_to_local2,file_name)

                #if the founded name belongs to a given file
                if os.path.isfile(file_path):
                    s3_key = os.path.join(s3_folder_prefix2,file_name)
                    s3.upload_file(file_path,bucket_name,s3_key)
                    print(f"uploaded the {file_name}-->s3://{bucket_name}/{s3_key}")
                    os.remove(file_path)
            print("all the files deleted")
        else:
            print("no files to delete")
    else:
        print("No such folder exists")
    
