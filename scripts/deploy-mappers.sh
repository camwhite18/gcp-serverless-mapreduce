#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topics and deploy the mappers
num_mappers=2
for ((i=0;i<num_mappers;i++)) do
  echo "Creating topic mapreduce-mapper-$i"
  if (gcloud pubsub topics create mapreduce-mapper-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-mapper-$i"
  else
    echo "Failed to create topic mapreduce-mapper-$i"
    exit 1
  fi

  echo "Deploying mapper $i"
  if (gcloud functions deploy mapper-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-mapper-"$i" \
  		--source=./map \
  		--entry-point Mapper \
  		--region=europe-west2 \
      --project=serverless-mapreduce) ; then
    echo "Successfully deployed mapper $i"
  else
    echo "Failed to deploy mapper $i"
    exit 1
  fi
done