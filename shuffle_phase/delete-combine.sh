#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the combine
echo "Deleting topic mapreduce-combine"
if (gcloud pubsub topics delete mapreduce-combine \
  --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-combine"
else
  echo "Failed to delete topic mapreduce-combine"
fi

echo "Deleting combine"
if (gcloud functions delete combine \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce) ; then
  echo "Successfully deleted combine"
else
  echo "Failed to delete combine"
fi
