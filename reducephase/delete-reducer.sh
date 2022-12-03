#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the reducers
echo "Deleting topic mapreduce-reducer"
if (gcloud pubsub topics delete mapreduce-reducer \
    --project="$GCP_PROJECT") ; then
  echo "Successfully deleted topic mapreduce-reducer"
else
  echo "Failed to delete topic mapreduce-reducer"
fi

echo "Deleting reducer"
if (gcloud functions delete reducer \
  --gen2 \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --quiet) ; then
  echo "Successfully deleted reducer"
else
  echo "Failed to delete reducer"
  exit 1
fi