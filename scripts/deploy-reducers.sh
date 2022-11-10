#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Deploy the reducers
num_reducers=5
for ((i=0;i<num_reducers;i++)) do
  echo "Creating topic mapreduce-reducer-$i"
  if (gcloud pubsub topics create mapreduce-reducer-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-reducer-$i"
  else
    echo "Failed to create topic mapreduce-reducer-$i"
    exit 1
  fi

  echo "Deploying reducer $i"
  if (gcloud functions deploy reducer-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-reducer-"$i" \
  		--source=. \
  		--entry-point Reducer \
  		--region=europe-west2 \
      --project=serverless-mapreduce) ; then
    echo "Successfully deployed reducer $i"
  else
    echo "Failed to deploy reducer $i"
    exit 1
  fi
done