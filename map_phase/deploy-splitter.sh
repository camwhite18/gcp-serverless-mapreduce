#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the splitter
echo "Creating topic mapreduce-splitter"
if (gcloud pubsub topics create mapreduce-splitter \
  --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-splitter"
else
  echo "Failed to create topic mapreduce-splitter"
  exit 1
fi

echo "Deploying splitter"
if (gcloud functions deploy splitter \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-splitter \
    --source=. \
    --entry-point Splitter \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT") ; then
  echo "Successfully deployed splitter"
else
  echo "Failed to deploy splitter"
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-splitter" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s
