#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the splitter
echo "Deleting topic mapreduce-splitter"
if (gcloud pubsub topics delete mapreduce-splitter \
  --project="$GCP_PROJECT") ; then
  echo "Successfully deleted topic mapreduce-splitter"
else
  echo "Failed to delete topic mapreduce-splitter"
fi

echo "Deleting splitter"
if (gcloud functions delete splitter \
  --gen2 \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT" \
  --quiet) ; then
  echo "Successfully deleted splitter"
else
  echo "Failed to delete splitter"
fi
