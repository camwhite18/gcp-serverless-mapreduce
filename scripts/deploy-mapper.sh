#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the mapper
echo "Creating topic mapreduce-mapper"
if (gcloud pubsub topics create mapreduce-mapper \
  --project=serverless-mapreduce) ; then
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
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
  echo "Successfully deployed mapper"
else
  echo "Failed to deploy mapper"
fi
