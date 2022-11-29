#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

echo "Deleting init-mapreduce"
if (gcloud functions delete init-mapreduce \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce) ; then
  echo "Successfully deleted init-mapreduce"
else
  echo "Failed to delete init-mapreduce"
fi
