#!/bin/bash

LAMBDA_FUNCTION_NAME="upload_cost_point"
AWS_REGION="eu-central-1"
NGROK_PORT=8080

# Cleanup function
cleanup() {
  echo -e "\n🛑 Stopping ngrok..."
  kill $NGROK_PID 2>/dev/null
  exit 0
}
trap cleanup SIGINT SIGTERM

echo "🚀 Starting ngrok on port $NGROK_PORT..."

# Start ngrok in background
ngrok http $NGROK_PORT > /tmp/ngrok.log 2>&1 &
NGROK_PID=$!
sleep 6

# Get URL
NGROK_URL=$(curl -s http://127.0.0.1:4040/api/tunnels | python3 -c "import sys, json; print(json.load(sys.stdin)['tunnels'][0]['public_url'])" 2>/dev/null)

if [[ -z "$NGROK_URL" ]]; then
  echo "❌ Failed to get ngrok URL:"
  cat /tmp/ngrok.log
  exit 1
fi

echo "✅ ngrok URL: $NGROK_URL"

# Fetch current env vars
CURRENT=$(aws lambda get-function-configuration --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" 2>/dev/null)
if [ $? -ne 0 ]; then
  echo "❌ Failed to fetch Lambda config. Check name/region."
  kill $NGROK_PID
  exit 1
fi

DAG_ID=$(echo "$CURRENT" | jq -r '.Environment.Variables.DAG_ID // "process_s3_csv"')
USER=$(echo "$CURRENT" | jq -r '.Environment.Variables.AIRFLOW_USER // "airflow"')
PASS=$(echo "$CURRENT" | jq -r '.Environment.Variables.AIRFLOW_PASSWORD // "airflow"')

# Update Lambda with JSON
ENV_JSON=$(jq -n \
  --arg url "$NGROK_URL" \
  --arg dag "$DAG_ID" \
  --arg user "$USER" \
  --arg pass "$PASS" \
  '{Variables: {AIRFLOW_URL: $url, DAG_ID: $dag, AIRFLOW_USER: $user, AIRFLOW_PASSWORD: $pass}}')

aws lambda update-function-configuration \
  --function-name "$LAMBDA_FUNCTION_NAME" \
  --region "$AWS_REGION" \
  --environment "$ENV_JSON" >/dev/null

echo "✅ Lambda updated!"
echo "🔗 Public URL: $NGROK_URL"
echo ""
echo "💡 Keep this terminal open. S3 uploads will trigger Airflow."
echo "   Press Ctrl+C to stop gracefully."

# Keep alive indefinitely
while true; do
  sleep 10
  # Optional: log heartbeat
  # echo "🔄 Tunnel still active: $NGROK_URL"
done