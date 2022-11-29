#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

echo "Deploying init-mapreduce"
if (gcloud functions deploy init-mapreduce \
    --gen2 \
    --runtime=go116 \
    --trigger-http \
    --source=. \
    --entry-point Service \
    --region=europe-west2 \
    --memory=512MB \
    --project=serverless-mapreduce) ; then
  echo "Successfully deployed init-mapreduce"
else
  echo "Failed to deploy init-mapreduce"
  exit 1
fi
