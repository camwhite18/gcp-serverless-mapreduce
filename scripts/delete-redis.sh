#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi
( \
num_reducers=5
for ((i=0;i<num_reducers;i++)) do
  ( \
  echo "Deleting Redis instance mapreduce-redis-$i"
  if (gcloud redis instances delete mapreduce-redis-"$i" \
      --region=europe-west2 \
      --project=serverless-mapreduce \
      --quiet) ; then
    echo "Successfully deleted Redis instance mapreduce-redis-$i"
  else
    echo "Failed to delete Redis instance mapreduce-redis-$i"
  fi
  ) &
done
) &
( \
echo "Deleting Redis instance mapreduce-controller"
if (gcloud redis instances delete mapreduce-controller \
    --project=serverless-mapreduce \
    --region=europe-west2 \
    --quiet) ; then
  echo "Successfully deleted Redis instance mapreduce-controller"
else
  echo "Failed to delete Redis instance mapreduce-controller"
fi
) &
( \
# Delete the VPC connector
if (gcloud compute networks vpc-access connectors delete mapreduce-connector \
    --project=serverless-mapreduce \
    --region=europe-west2 \
    --quiet) ; then
  echo "Successfully deleted VPC connector"
else
  echo "Failed to delete VPC connector or it has already been deleted"
fi
) &
wait