#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the reducers
echo "Deleting topic mapreduce-reducer"
if (gcloud pubsub topics delete mapreduce-reducer \
    --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-reducer"
else
  echo "Failed to delete topic mapreduce-reducer"
fi

echo "Deleting reducer"
if (gcloud functions delete reducer \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce \
  --quiet) ; then
  echo "Successfully deleted reducer"
else
  echo "Failed to delete reducer"
  exit 1
fi