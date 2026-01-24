# upload.py
import boto3
import requests
from datetime import datetime
import uuid

# --- Step 1: Upload to S3 ---
session = boto3.Session(profile_name='file-uploader')
s3 = session.client('s3')

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
unique_id = str(uuid.uuid4())[:8]
key = f"uploads/price_points_{timestamp}_{unique_id}.csv"

s3.put_object(
    Bucket='cost-point',
    Key=key,
    Body=b'name,age\nAlice,30'
)
print(f"✅ Uploaded to s3://cost-point/{key}")

# --- Step 2: Trigger Airflow DAG (just like your curl) ---
AIRFLOW_URL = "http://localhost:8080"  # or your ngrok URL
DAG_ID = "process_s3_csv"
USERNAME = "airflow"
PASSWORD = "airflow"

payload = {
    "conf": {
        "s3_bucket": "cost-point",
        "s3_key": key
    }
}

response = requests.post(
    f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns",
    json=payload,
    auth=(USERNAME, PASSWORD),
    headers={"Content-Type": "application/json"}
)

if response.status_code == 200:
    print("✅ Airflow DAG triggered successfully!")
else:
    print(f"❌ Failed to trigger DAG: {response.status_code} - {response.text}")