import boto3
import os
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION")

s3_client = boto3.client("s3", region_name=REGION)

def upload_file_to_s3(local_file, bucket, s3_path):
    """
    Upload a local file to S3
    """
    try:
        s3_client.upload_file(local_file, bucket, s3_path)
        print(f"Uploaded {local_file} to s3://{bucket}/{s3_path}")
    except Exception as e:
        print("Error uploading to S3:", e)

def download_file_from_s3(bucket, s3_path, local_file):
    """
    Download a file from S3
    """
    try:
        s3_client.download_file(bucket, s3_path, local_file)
        print(f"Downloaded s3://{bucket}/{s3_path} to {local_file}")
    except Exception as e:
        print("Error downloading from S3:", e)
