#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the shufflers
num_shufflers=5
for ((i=0;i<num_shufflers;i++)) do
  echo "Deleting topic mapreduce-shuffler-$i"
  if (gcloud pubsub topics delete mapreduce-shuffler-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully deleted topic mapreduce-shuffler-$i"
  else
    echo "Failed to delete topic mapreduce-shuffler-$i"
  fi

  echo "Deleting shuffler $i"
  if (gcloud functions delete shuffler-"$i" \
    --gen2 \
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
    echo "Successfully deleted shuffler $i"
  else
    echo "Failed to delete shuffler $i"
    exit 1
  fi

  echo "Deleting Redis instance mapreduce-shuffler-$i"
    if (gcloud redis instances delete mapreduce-shuffler-"$i" \
        --project=serverless-mapreduce \
        --region=europe-west2) ; then
      echo "Successfully deleted Redis instance mapreduce-shuffler-$i"
    else
      echo "Failed to delete Redis instance mapreduce-shuffler-$i"
    fi
done

# Delete the VPC connector
if (gcloud compute networks vpc-access connectors delete mapreduce-connector \
    --project=serverless-mapreduce \
    --region=europe-west2) ; then
  echo "Successfully deleted VPC connector"
else
  echo "Failed to delete VPC connector"
fi