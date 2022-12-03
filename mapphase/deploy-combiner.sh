#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the combiner
echo "Creating topic mapreduce-combiner"
if (gcloud pubsub topics create mapreduce-combiner \
  --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-combiner"
else
  echo "Failed to create topic mapreduce-combiner"
  exit 1
fi

echo "Deploying combiner"
if (gcloud functions deploy combiner \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-combiner \
    --source=. \
    --entry-point Combiner \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT" \
    --set-env-vars=GCP_PROJECT="$GCP_PROJECT") ; then
  echo "Successfully deployed combiner"
else
  echo "Failed to deploy combiner"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-combiner" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s
