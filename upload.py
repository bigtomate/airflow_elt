# upload.py
import boto3
import requests
from botocore.exceptions import ClientError

# --- Configuration ---
BUCKET = 'cost-point'
KEY = 'uploads/price_points_1.csv'
AIRFLOW_URL = "http://localhost:8080"
DAG_ID = "process_s3_csv"
USERNAME = "airflow"
PASSWORD = "airflow"

# --- Step 1: Upload to S3 ---
# Uses AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from environment
s3 = boto3.client('s3')

s3.put_object(
    Bucket=BUCKET,
    Key=KEY,
    Body=b'name,age\nAlice,30'
)
print(f"✅ Uploaded to s3://{BUCKET}/{KEY}")

# --- Step 2: Check if already processed ---
try:
    tagging = s3.get_object_tagging(Bucket=BUCKET, Key=KEY)
    tags = {tag['Key']: tag['Value'] for tag in tagging.get('TagSet', [])}
    if tags.get('processed') == 'true':
        print("⏭️ File is already processed. Skipping DAG trigger.")
        exit(0)
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == 'NoSuchKey':
        print(f"❌ File not found in S3: s3://{BUCKET}/{KEY}")
        exit(1)
    elif error_code == 'AccessDenied':
        print("⚠️ Warning: Unable to read S3 tags (missing GetObjectTagging permission). Proceeding with DAG trigger.")
    else:
        print(f"⚠️ Unexpected error checking tags: {e}. Proceeding.")

# --- Step 3: Trigger Airflow DAG ---
payload = {
    "conf": {
        "s3_bucket": BUCKET,
        "s3_key": KEY
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