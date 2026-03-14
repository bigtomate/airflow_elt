import json
import os
import urllib3
from base64 import b64encode

# Get config from environment variables
AIRFLOW_URL = os.environ['AIRFLOW_URL']
DAG_ID = os.environ['DAG_ID']
USERNAME = os.environ['AIRFLOW_USER']
PASSWORD = os.environ['AIRFLOW_PASSWORD']

http = urllib3.PoolManager()


def lambda_handler(event, context):
    # Extract S3 info
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    print(f"Triggering Airflow DAG for s3://{bucket}/{key}")

    # Payload to send to Airflow
    payload = {
        "conf": {
            "s3_bucket": bucket,
            "s3_key": key
        }
    }

    # Basic auth
    credentials = b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode("ascii")

    try:
        resp = http.request(
            "POST",
            f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns",
            body=json.dumps(payload),
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json"
            }
        )
        print("Airflow response:", resp.status, resp.data.decode())
        return {"statusCode": resp.status}

    except Exception as e:
        print("Error:", str(e))
        raise  # ← Just 'raise', nothing after it!

