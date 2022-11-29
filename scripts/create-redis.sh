#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

#Create VPC connector for serverless VPC access to Redis
if (gcloud compute networks vpc-access connectors create mapreduce-connector \
    --project=serverless-mapreduce \
    --network=default \
    --region=europe-west2 \
    --max-instances=3 \
    --range=10.8.0.0/28) ; then
  echo "Successfully created VPC connector"
else
  echo "Failed to create VPC connector or it already exists"
fi

echo "Creating Redis instance mapreduce-controller"
if (gcloud redis instances create mapreduce-controller \
    --tier=basic \
    --region=europe-west2 \
    --size=1 \
    --network=default) ; then
  echo "Successfully created Redis instance mapreduce-controller"
else
  echo "Failed to create Redis instance mapreduce-controller="
  exit 1
fi

num_reducers=5
for ((i=0;i<num_reducers;i++)) do
  ( \
  echo "Creating Redis instance mapreduce-redis-$i"
  if (gcloud redis instances create mapreduce-redis-"$i" \
      --tier=basic \
      --region=europe-west2 \
      --size=1 \
      --network=default) ; then
    echo "Successfully created Redis instance mapreduce-redis-$i"
  else
    echo "Failed to create Redis instance mapreduce-redis-$i"
    exit 1
  fi
  ) &
done; wait
