#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the combine
echo "Deleting topic mapreduce-combiner"
if (gcloud pubsub topics delete mapreduce-combiner \
  --project="$GCP_PROJECT") ; then
  echo "Successfully deleted topic mapreduce-combiner"
else
  echo "Failed to delete topic mapreduce-combiner"
fi

echo "Deleting combine"
if (gcloud functions delete combiner \
  --gen2 \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --quiet) ; then
  echo "Successfully deleted combiner"
else
  echo "Failed to delete combiner"
fi
