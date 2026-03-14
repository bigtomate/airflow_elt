from datetime import datetime
import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

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

        # --- Step 1: Check if already processed (with error handling) ---
        try:
            tagging = s3.get_object_tagging(Bucket=s3_bucket, Key=s3_key)
            tag_dict = {t['Key']: t['Value'] for t in tagging.get('TagSet', [])}
            if tag_dict.get('processed') == 'true':
                logger.info(f"✅ SKIPPED: {full_key} is already processed.")
                raise AirflowSkipException(f"File {full_key} already processed.")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise ValueError(f"❌ File not found: {full_key}")
            elif error_code == 'AccessDenied':
                # Can't read tags → assume unprocessed BUT log warning
                logger.warning(f"⚠️ Unable to check tags on {full_key} (AccessDenied). Proceeding with caution.")
            else:
                logger.warning(f"⚠️ Unexpected error checking tags: {e}. Proceeding.")

        # --- Step 2: Download and process ---
        logger.info(f"📥 Downloading {full_key}...")
        try:
            response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise ValueError(f"❌ File disappeared during processing: {full_key}")
            raise

        logger.info("=== CSV CONTENT ===")
        logger.info(content)
        logger.info("=== END OF CSV ===")

        line_count = len(content.splitlines())
        logger.info(f"🎉 Processed {full_key} ({line_count} lines).")

        # --- Step 3: Mark as processed (best-effort) ---
        try:
            s3.put_object_tagging(
                Bucket=s3_bucket,
                Key=s3_key,
                Tagging={'TagSet': [{'Key': 'processed', 'Value': 'true'}]}
            )
            logger.info(f"✅ Tagged {full_key} as processed.")
        except ClientError as e:
            # Log but DON'T fail the task — we don't want to reprocess successfully handled data!
            logger.error(f"❌ Failed to tag {full_key} as processed (IAM issue?): {e}")
            # Optional: send alert via email/Slack here
        except Exception as e:
            logger.error(f"❌ Unexpected tagging error: {e}")

        return line_count

    download_and_print_csv(
        s3_bucket="{{ dag_run.conf.s3_bucket }}",
        s3_key="{{ dag_run.conf.s3_key }}"
    )

process_s3_csv()