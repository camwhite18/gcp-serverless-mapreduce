#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the controller
echo "Deleting topic mapreduce-controller"
if (gcloud pubsub topics delete mapreduce-controller \
    --project=serverless-mapreduce) ; then
  echo "Successfully deleted topic mapreduce-controller"
else
  echo "Failed to delete topic mapreduce-controller"
fi

echo "Deleting controller"
if (gcloud functions delete controller \
  --gen2 \
  --region=europe-west2 \
  --project=serverless-mapreduce \
  --quiet) ; then
  echo "Successfully deleted controller"
else
  echo "Failed to delete controller"
  exit 1
fi

echo "Deleting Redis instance mapreduce-controller"
if (gcloud redis instances delete mapreduce-controller \
    --project=serverless-mapreduce \
    --region=europe-west2 \
    --quiet) ; then
  echo "Successfully deleted Redis instance mapreduce-controller"
else
  echo "Failed to delete Redis instance mapreduce-controller"
fi

# Delete the VPC connector
if (gcloud compute networks vpc-access connectors delete mapreduce-connector \
    --project=serverless-mapreduce \
    --region=europe-west2) ; then
  echo "Successfully deleted VPC connector"
else
  echo "Failed to delete VPC connector or it has already been deleted"
fi