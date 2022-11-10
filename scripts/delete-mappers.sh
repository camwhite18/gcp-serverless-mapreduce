#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the mappers
num_mappers=10
for ((i=0;i<num_mappers;i++)) do
  echo "Deleting topic mapreduce-mapper-$i"
  if (gcloud pubsub topics delete mapreduce-mapper-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully deleted topic mapreduce-mapper-$i"
  else
    echo "Failed to delete topic mapreduce-mapper-$i"
  fi

  echo "Deleting mapper $i"
  if (gcloud functions delete mapper-"$i" \
    --gen2 \
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
    echo "Successfully deleted mapper $i"
  else
    echo "Failed to delete mapper $i"
    exit 1
  fi
done