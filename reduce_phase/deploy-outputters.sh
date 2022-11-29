#!/usr/bin/env bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create VPC connector for serverless VPC access to Redis
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

# Create the topics, redis instances and deploy the mappers
num_outputters=5
for ((i=0;i<num_outputters;i++)) do
  ( \
  echo "Creating topic mapreduce-outputter-$i"
  if (gcloud pubsub topics create mapreduce-outputter-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-outputter-$i"
  else
    echo "Failed to create topic mapreduce-outputter-$i"
    exit 1
  fi

  REDIS_HOST=$(gcloud redis instances describe mapreduce-reducer-"$i" \
                --region=europe-west2 \
                --format="value(host)")
  REDIS_PORT=$(gcloud redis instances describe mapreduce-reducer-"$i" \
                --region=europe-west2 \
                --format="value(port)")

  echo "Deploying outputter $i"
  if (gcloud functions deploy outputter-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-outputter-"$i" \
  		--source=. \
  		--entry-point Outputter \
  		--region=europe-west2 \
  		--memory=512MB \
      --project=serverless-mapreduce \
      --vpc-connector=projects/serverless-mapreduce/locations/europe-west2/connectors/mapreduce-connector \
      --set-env-vars=REDIS_HOST="$REDIS_HOST",REDIS_PORT="$REDIS_PORT"
      ) ; then
    echo "Successfully deployed outputter $i"
  else
    echo "Failed to deploy outputter $i"
    exit 1
  fi

  # Change the backoff delay of the subscription to start at 1 second
  subscription=$(gcloud pubsub subscriptions list | grep "eventarc-europe-west2-outputter-$i" | cut -c 7-)
  echo "Changing backoff delay of subscription $subscription"
  gcloud pubsub subscriptions update "$subscription" \
    --project=serverless-mapreduce \
    --min-retry-delay=1s \
    --max-retry-delay=10s
  ) &
done; wait