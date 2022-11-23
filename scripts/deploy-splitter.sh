#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the splitter
echo "Creating topic mapreduce-splitter"
if (gcloud pubsub topics create mapreduce-splitter \
  --project=serverless-mapreduce) ; then
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
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
  echo "Successfully deployed splitter"
else
  echo "Failed to deploy splitter"
fi
