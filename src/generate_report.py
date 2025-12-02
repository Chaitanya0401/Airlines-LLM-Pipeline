import os
import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from io import StringIO


# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET_NAME")   # only ONE bucket now
REPORT_PATH = "reports/"               # reports stored inside same bucket

if not BUCKET:
    raise ValueError("❌ S3_BUCKET_NAME missing in .env")

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_latest_processed_file(bucket):
    """Return the latest CSV or JSON inside processed/ folder."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix="processed/")

    if "Contents" not in response:
        raise FileNotFoundError("❌ No processed files found in S3.")

    files = [
        item for item in response["Contents"]
        if item["Key"].endswith(".csv") or item["Key"].endswith(".json")
    ]

    if not files:
        raise FileNotFoundError("❌ No processed CSV/JSON found.")

    latest = max(files, key=lambda x: x["LastModified"])
    return latest["Key"]


def load_processed_dataset(bucket, key):
    """Load CSV or JSON as Pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_data = obj["Body"].read().decode("utf-8")

    if key.endswith(".csv"):
        return pd.read_csv(StringIO(raw_data))
    elif key.endswith(".json"):
        return pd.read_json(StringIO(raw_data))
    else:
        raise ValueError("Unsupported file type")


def generate_summary_llm(df: pd.DataFrame):
    """Create structured aviation insights report."""
    preview_data = df.head(50).to_string(index=False)

    template = """
You are an aviation analyst. Based on the structured flight dataset below,
generate a clear, structured aviation summary report.

Dataset:
{dataset}

Your report MUST include:

- Total number of flights
- Top 3 departure airports
- Top 3 arrival airports
- Airline distribution
- Flight status breakdown (scheduled, landed, delayed, cancelled)
- Any patterns or anomalies you detect

Write 3–6 short paragraphs (no bullet points).
"""

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatOllama(model="llama2", temperature=0.3)

    chain = prompt | llm
    result = chain.invoke({"dataset": preview_data})

    return result.content


def upload_report_to_s3(report_text):
    """Save report inside reports/ folder."""
    filename = f"reports/flight_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    s3.put_object(
        Bucket=BUCKET,
        Key=filename,
        Body=report_text,
        ContentType="text/plain"
    )

    return filename


def main():
    print("\n📁 Fetching latest processed dataset...")
    latest = get_latest_processed_file(BUCKET)
    print(f"➡ Latest file: {latest}")

    df = load_processed_dataset(BUCKET, latest)

    print("🧠 Running aviation analysis with Llama2 (Ollama)...")
    report = generate_summary_llm(df)

    print("📤 Uploading report to S3...")
    out = upload_report_to_s3(report)

    print(f"\n✅ Report saved as: {out}")
    print("Check the /reports/ folder in your S3 bucket.\n")


if __name__ == "__main__":
    main()
