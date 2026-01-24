# Dockerfile
FROM apache/airflow:2.10.1-python3.11

# Install uv (using pip, ironically — but only once)
RUN pip install uv

# Use uv to install your extra packages (much faster than pip!)
# --system installs into the system Python environment (required in this image)
RUN uv pip install --system boto3 apache-airflow-providers-amazon