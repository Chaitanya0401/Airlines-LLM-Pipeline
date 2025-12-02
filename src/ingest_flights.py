import os
import requests
from dotenv import load_dotenv
from s3_utils import upload_file_to_s3
from datetime import datetime
import json

load_dotenv()

API_KEY = os.getenv("AVIATIONSTACK_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_REGION")

def fetch_flights(limit=2):
    url = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}&limit={limit}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.text)
        return None

def save_to_s3(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"flights_{timestamp}.json"
    # Save as proper JSON
    with open(file_name, "w") as f:
        json.dump(data, f, indent=2)   
    upload_file_to_s3(file_name, S3_BUCKET, f"raw/{file_name}")

if __name__ == "__main__":
    data = fetch_flights()
    if data:
        save_to_s3(data)
        print("Ingestion complete!")
