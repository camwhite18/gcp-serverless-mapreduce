#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

uri=$(gcloud functions describe starter --region="$GCP_REGION" --project="$GCP_PROJECT" --format="value(serviceConfig.uri)")

echo "Enter input bucket name:"
read -r input_bucket
echo "Enter output bucket name:"
read -r output_bucket

curl -X GET "$uri?input-bucket=$input_bucket&output-bucket=$output_bucket"