import os
import json
import boto3
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

load_dotenv()

# Environment variables
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_REGION")

# Initialize S3 client
s3_client = boto3.client("s3", region_name=REGION)


def list_raw_files(prefix="raw/"):
    """
    List all raw flight files in S3 under `raw/`
    """
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = []
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".json"):
            files.append(obj["Key"])
    return files


def download_file_from_s3(s3_key, local_file):
    """
    Download a file from S3 to local
    """
    s3_client.download_file(S3_BUCKET, s3_key, local_file)
    print(f"Downloaded {s3_key} to {local_file}")


def clean_flight_data(raw_data):
    """
    Transform raw AviationStack JSON into structured data
    """
    cleaned = []
    for flight in raw_data.get("data", []):
        cleaned.append({
            "flight_date": flight.get("flight_date"),
            "flight_status": flight.get("flight_status"),
            "departure_airport": flight.get("departure", {}).get("airport", "Unknown"),
            "departure_iata": flight.get("departure", {}).get("iata", "Unknown"),
            "arrival_airport": flight.get("arrival", {}).get("airport", "Unknown"),
            "arrival_iata": flight.get("arrival", {}).get("iata", "Unknown"),
            "airline": flight.get("airline", {}).get("name", "Unknown"),
            "flight_number": flight.get("flight", {}).get("iata", "Unknown")
        })
    return cleaned


def upload_file_to_s3(local_file, s3_path):
    """
    Upload local file to S3
    """
    s3_client.upload_file(local_file, S3_BUCKET, s3_path)
    print(f"Uploaded {local_file} to s3://{S3_BUCKET}/{s3_path}")


def main():
    raw_files = list_raw_files()
    if not raw_files:
        print("No raw files found in S3!")
        return

    for raw_file in raw_files:
        local_raw = raw_file.split("/")[-1]
        download_file_from_s3(raw_file, local_raw)

        # Load raw JSON
        with open(local_raw, "r") as f:
            raw_data = json.load(f)

        # Clean data
        cleaned_data = clean_flight_data(raw_data)

        # Save cleaned JSON locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_file = f"flights_cleaned_{timestamp}.json"
        with open(cleaned_file, "w") as f:
            json.dump(cleaned_data, f, indent=2)

        # Upload cleaned JSON to S3
        upload_file_to_s3(cleaned_file, f"processed/{cleaned_file}")

        # Optional: save CSV version
        csv_file = f"flights_cleaned_{timestamp}.csv"
        df = pd.DataFrame(cleaned_data)
        df.to_csv(csv_file, index=False)
        upload_file_to_s3(csv_file, f"processed/{csv_file}")

    print("Phase 2 transformation complete!")


if __name__ == "__main__":
    main()
