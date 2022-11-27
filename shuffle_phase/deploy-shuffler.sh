#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the shuffler
echo "Creating topic mapreduce-shuffler"
if (gcloud pubsub topics create mapreduce-shuffler \
  --project=serverless-mapreduce) ; then
  echo "Successfully created topic mapreduce-shuffler"
else
  echo "Failed to create topic mapreduce-shuffler"
  exit 1
fi

echo "Deploying shuffler"
if (gcloud functions deploy shuffler \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-shuffler \
    --source=. \
    --entry-point Shuffler \
    --region=europe-west2 \
    --memory=512MB \
    --project=serverless-mapreduce) ; then
  echo "Successfully deployed shuffler"
else
  echo "Failed to deploy shuffler"
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-europe-west2-shuffler" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project=serverless-mapreduce \
  --min-retry-delay=1s \
  --max-retry-delay=10s
