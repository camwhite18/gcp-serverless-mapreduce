#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the mapper
echo "Deleting topic mapreduce-mapper"
if (gcloud pubsub topics delete mapreduce-mapper \
  --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-mapper"
else
  echo "Failed to delete topic mapreduce-mapper"
fi

echo "Deleting mapper"
if (gcloud functions delete mapper \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce \
  --quiet) ; then
  echo "Successfully deleted mapper"
else
  echo "Failed to delete mapper"
fi
