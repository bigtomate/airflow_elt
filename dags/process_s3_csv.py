# dags/process_s3_csv.py
from datetime import datetime
import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from botocore.exceptions import ClientError


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "idempotent"]
)
def process_s3_csv():
    @task
    def download_and_print_csv(s3_bucket: str, s3_key: str):
        s3 = boto3.client("s3")
        full_key = f"s3://{s3_bucket}/{s3_key}"

        # --- Check if already processed ---
        try:
            tagging = s3.get_object_tagging(Bucket=s3_bucket, Key=s3_key)
            tag_dict = {t['Key']: t['Value'] for t in tagging.get('TagSet', [])}
            print('print tags ', tag_dict)
            if tag_dict.get('processed') == 'true':
                print(f"✅ SKIPPED: {full_key} is already processed.")
                raise AirflowSkipException(f"File {full_key} already processed.")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise ValueError(f"❌ File not found: {full_key}")
            else:
                print(f"⚠️  Unable to read tags (e.g., no permissions). Assuming unprocessed.")

        # --- Download and process ---
        print(f"📥 Downloading {full_key}...")
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        content = response["Body"].read().decode("utf-8")

        print("=== CSV CONTENT ===")
        print(content)
        print("=== END OF CSV ===")

        # --- Mark as processed ---
        try:
            s3.put_object_tagging(
                Bucket=s3_bucket,
                Key=s3_key,
                Tagging={'TagSet': [{'Key': 'processed', 'Value': 'true'}]}
            )
            print(f"✅ Tagged {full_key} as processed.")
        except Exception as e:
            print(f"❌ Failed to tag object: {e}")
            raise  # Fail task so you notice

        line_count = len(content.splitlines())
        print(f"🎉 Successfully processed {full_key} ({line_count} lines).")
        return line_count

    download_and_print_csv(
        s3_bucket="{{ dag_run.conf.s3_bucket }}",
        s3_key="{{ dag_run.conf.s3_key }}"
    )


process_s3_csv()