#!/usr/bin/env bash

source .env

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
      --region="$GCP_REGION" \
      --project="$GCP_PROJECT" \
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
    --project="$GCP_PROJECT" \
    --region="$GCP_REGION" \
    --quiet) ; then
  echo "Successfully deleted Redis instance mapreduce-controller"
else
  echo "Failed to delete Redis instance mapreduce-controller"
fi
) &
( \
# Delete the VPC connector
if (gcloud compute networks vpc-access connectors delete mapreduce-connector \
    --project="$GCP_PROJECT" \
    --region="$GCP_REGION" \
    --quiet) ; then
  echo "Successfully deleted VPC connector"
else
  echo "Failed to delete VPC connector or it has already been deleted"
fi
) &
wait