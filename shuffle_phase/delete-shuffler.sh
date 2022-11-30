#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the shuffler
echo "Deleting topic mapreduce-shuffler"
if (gcloud pubsub topics delete mapreduce-shuffler \
  --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-shuffler"
else
  echo "Failed to delete topic mapreduce-shuffler"
fi

echo "Deleting shuffler"
if (gcloud functions delete shuffler \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce \
  --quiet) ; then
  echo "Successfully deleted shuffler"
else
  echo "Failed to delete shuffler"
fi
