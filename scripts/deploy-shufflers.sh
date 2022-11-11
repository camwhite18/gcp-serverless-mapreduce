#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topics and deploy the shufflers
num_shufflers=5
for ((i=0;i<num_shufflers;i++)) do
  echo "Creating topic mapreduce-shuffler-$i"
  if (gcloud pubsub topics create mapreduce-shuffler-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-shuffler-$i"
  else
    echo "Failed to create topic mapreduce-shuffler-$i"
    exit 1
  fi

  echo "Deploying shuffler $i"
  if (gcloud functions deploy shuffler-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-shuffler-"$i" \
  		--source=./shuffle \
  		--entry-point Shuffler \
  		--region=europe-west2 \
      --project=serverless-mapreduce) ; then
    echo "Successfully deployed shuffler $i"
  else
    echo "Failed to deploy shuffler $i"
    exit 1
  fi
done