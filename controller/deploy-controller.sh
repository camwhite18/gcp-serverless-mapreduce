#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic, redis instance and deploy the controller
echo "Creating topic mapreduce-controller"
if (gcloud pubsub topics create mapreduce-controller \
    --project=serverless-mapreduce) ; then
  echo "Successfully created topic mapreduce-controller"
else
  echo "Failed to create topic mapreduce-controller"
  exit 1
fi

REDIS_HOST=$(gcloud redis instances describe mapreduce-controller \
              --region=europe-west2 \
              --format="value(host)")

echo "Deploying controller"
if (gcloud functions deploy controller \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-controller \
    --source=. \
    --entry-point Controller \
    --region=europe-west2 \
    --memory=512MB \
    --project=serverless-mapreduce \
    --vpc-connector=projects/serverless-mapreduce/locations/europe-west2/connectors/mapreduce-connector \
    --set-env-vars=REDIS_HOST="$REDIS_HOST"
    ) ; then
  echo "Successfully deployed controller"
else
  echo "Failed to deploy controller"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-europe-west2-controller" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project=serverless-mapreduce \
  --min-retry-delay=1s \
  --max-retry-delay=10s