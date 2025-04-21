import boto3
import os


def files_checker():
    return (os.listdir("/home/ec2-user")) >0



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
