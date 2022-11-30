#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the mapper
echo "Creating topic mapreduce-mapper"
if (gcloud pubsub topics create mapreduce-mapper \
  --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-mapper"
else
  echo "Failed to create topic mapreduce-mapper"
  exit 1
fi

echo "Deploying mapper"
if (gcloud functions deploy mapper \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-mapper \
    --source=. \
    --entry-point Mapper \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT") ; then
  echo "Successfully deployed mapper"
else
  echo "Failed to deploy mapper"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-mapper" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s
