#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

echo "Deploying starter"
if (gcloud functions deploy starter \
    --gen2 \
    --runtime=go116 \
    --trigger-http \
    --source=. \
    --entry-point Starter \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT" \
    --set-env-vars=GCP_PROJECT="$GCP_PROJECT") ; then
  echo "Successfully deployed starter"
else
  echo "Failed to deploy starter"
  exit 1
fi
