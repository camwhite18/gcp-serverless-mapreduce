#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the reducers
num_reducers=5
for ((i=0;i<num_reducers;i++)) do
  ( \
  echo "Deleting topic mapreduce-reducer-$i"
  if (gcloud pubsub topics delete mapreduce-reducer-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully deleted topic mapreduce-reducer-$i"
  else
    echo "Failed to delete topic mapreduce-reducer-$i"
  fi

  echo "Deleting reducer $i"
  if (gcloud functions delete reducer-"$i" \
    --gen2 \
    --region=europe-west2 \
    --project=serverless-mapreduce \
    --quiet) ; then
    echo "Successfully deleted reducer $i"
  else
    echo "Failed to delete reducer $i"
    exit 1
  fi

  echo "Deleting Redis instance mapreduce-reducer-$i"
  if (gcloud redis instances delete mapreduce-reducer-"$i" \
      --project=serverless-mapreduce \
      --region=europe-west2 \
      --quiet) ; then
    echo "Successfully deleted Redis instance mapreduce-reducer-$i"
  else
    echo "Failed to delete Redis instance mapreduce-reducer-$i"
  fi
  ) &
done; wait

# Delete the VPC connector
if (gcloud compute networks vpc-access connectors delete mapreduce-connector \
    --project=serverless-mapreduce \
    --region=europe-west2) ; then
  echo "Successfully deleted VPC connector"
else
  echo "Failed to delete VPC connector"
fi