#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

echo "Deleting init-mapreduce"
if (gcloud functions delete init-mapreduce \
  --gen2 \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --quiet) ; then
  echo "Successfully deleted init-mapreduce"
else
  echo "Failed to delete init-mapreduce"
fi
