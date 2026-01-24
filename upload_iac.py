# upload.py
import boto3
import os
from datetime import datetime
import uuid

# Read from environment (set by .env or shell)
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

bucket = os.getenv('S3_BUCKET')
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
unique_id = str(uuid.uuid4())[:8]
key = f"uploads/test-trigger_{timestamp}_{unique_id}.csv"

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=b'name,age\nAlice,30'
)
print(f"✅ Uploaded to s3://{bucket}/{key}")