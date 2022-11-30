#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the controller
echo "Deleting topic mapreduce-controller"
if (gcloud pubsub topics delete mapreduce-controller \
    --project="$GCP_PROJECT") ; then
  echo "Successfully deleted topic mapreduce-controller"
else
  echo "Failed to delete topic mapreduce-controller"
fi

echo "Deleting controller"
if (gcloud functions delete controller \
  --gen2 \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --quiet) ; then
  echo "Successfully deleted controller"
else
  echo "Failed to delete controller"
  exit 1
fi
