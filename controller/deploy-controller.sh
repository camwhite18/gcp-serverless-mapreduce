#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic, redis instance and deploy the controller
echo "Creating topic mapreduce-controller"
if (gcloud pubsub topics create mapreduce-controller \
    --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-controller"
else
  echo "Failed to create topic mapreduce-controller"
  exit 1
fi

REDIS_HOST=$(gcloud redis instances describe mapreduce-controller \
              --region="$GCP_REGION" \
              --format="value(host)")

echo "Deploying controller"
if (gcloud functions deploy controller \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-controller \
    --source=. \
    --entry-point Controller \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT" \
    --vpc-connector=projects/"$GCP_PROJECT"/locations/"$GCP_REGION"/connectors/mapreduce-connector \
    --set-env-vars=REDIS_HOST="$REDIS_HOST"
    ) ; then
  echo "Successfully deployed controller"
else
  echo "Failed to deploy controller"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-controller" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s