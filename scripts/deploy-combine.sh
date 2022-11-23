#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the combine
echo "Creating topic mapreduce-combine"
if (gcloud pubsub topics create mapreduce-combine \
  --project=serverless-mapreduce) ; then
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
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
  echo "Successfully deployed combine"
else
  echo "Failed to deploy combine"
fi
