#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the controller
echo "Deleting topic mapreduce-controller"
if (gcloud pubsub topics delete mapreduce-controller \
    --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-controller"
else
  echo "Failed to delete topic mapreduce-controller"
fi

echo "Deleting controller"
if (gcloud functions delete controller \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce \
  --quiet) ; then
  echo "Successfully deleted controller"
else
  echo "Failed to delete controller"
  exit 1
fi
