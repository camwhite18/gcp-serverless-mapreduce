#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create VPC connector for serverless VPC access to Redis
#if (gcloud compute networks vpc-access connectors create mapreduce-connector \
#    --project=serverless-mapreduce \
#    --network=default \
#    --region=europe-west2 \
#    --max-instances=3 \
#    --range=10.8.0.0/28) ; then
#  echo "Successfully created VPC connector"
#else
#  echo "Failed to create VPC connector"
#  exit 1
#fi

# Create the topics, redis instances and deploy the mappers
num_shufflers=1
for ((i=0;i<num_shufflers;i++)) do
  echo "Creating topic mapreduce-shuffler-$i"
  if (gcloud pubsub topics create mapreduce-shuffler-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-shuffler-$i"
  else
    echo "Failed to create topic mapreduce-shuffler-$i"
    exit 1
  fi

#  echo "Creating Redis instance mapreduce-shuffler-$i"
#  if (gcloud redis instances create mapreduce-shuffler-"$i" \
#      --tier=basic \
#      --region=europe-west2 \
#      --size=1 \
#      --network=default) ; then
#    echo "Successfully created Redis instance mapreduce-shuffler-$i"
#  else
#    echo "Failed to create Redis instance mapreduce-shuffler-$i"
#    exit 1
#  fi

#  REDIS_HOST=$(gcloud redis instances describe mapreduce-shuffler-"$i" \
#                --region=europe-west2 \
#                --format="value(host)")
#  REDIS_PORT=$(gcloud redis instances describe mapreduce-shuffler-"$i" \
#                --region=europe-west2 \
#                --format="value(port)")

  echo "Deploying shuffler $i"
  if (gcloud functions deploy shuffler-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-shuffler-"$i" \
  		--source=. \
  		--entry-point Shuffler \
  		--region=europe-west2 \
  		--memory=512MB \
      --project=serverless-mapreduce \
#      --vpc-connector=projects/serverless-mapreduce/locations/europe-west2/connectors/mapreduce-connector \
#      --set-env-vars=REDIS_HOST="$REDIS_HOST",REDIS_PORT="$REDIS_PORT"
      ) ; then
    echo "Successfully deployed shuffler $i"
  else
    echo "Failed to deploy shuffler $i"
    exit 1
  fi
done