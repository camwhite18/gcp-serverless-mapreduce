#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topics and deploy the splitters
num_splitters=10
for ((i=0;i<num_splitters;i++)) do
  echo "Creating topic mapreduce-splitter-$i"
  if (gcloud pubsub topics create mapreduce-splitter-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-splitter-$i"
  else
    echo "Failed to create topic mapreduce-splitter-$i"
    exit 1
  fi

  echo "Deploying splitter $i"
  if (gcloud functions deploy splitter-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-splitter-"$i" \
  		--source=. \
  		--entry-point Splitter \
  		--region=europe-west2 \
      --project=serverless-mapreduce) ; then
    echo "Successfully deployed splitter $i"
  else
    echo "Failed to deploy splitter $i"
    exit 1
  fi
done