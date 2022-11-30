#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the combine
echo "Creating topic mapreduce-combine"
if (gcloud pubsub topics create mapreduce-combine \
  --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-combine"
else
  echo "Failed to create topic mapreduce-combine"
  exit 1
fi

echo "Deploying combine"
if (gcloud functions deploy combine \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-combine \
    --source=. \
    --entry-point Combine \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT") ; then
  echo "Successfully deployed combine"
else
  echo "Failed to deploy combine"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-combine" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s
