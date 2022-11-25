#!/bin/bash

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
  echo "Failed to create VPC connector"
  exit 1
fi

# Create the topics, redis instances and deploy the mappers
num_reducers=5
for ((i=1;i<num_reducers;i++)) do
  ( \
  echo "Creating topic mapreduce-reducer-$i"
  if (gcloud pubsub topics create mapreduce-reducer-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-reducer-$i"
  else
    echo "Failed to create topic mapreduce-reducer-$i"
    exit 1
  fi

  echo "Creating Redis instance mapreduce-reducer-$i"
  if (gcloud redis instances create mapreduce-reducer-"$i" \
      --tier=basic \
      --region=europe-west2 \
      --size=1 \
      --network=default) ; then
    echo "Successfully created Redis instance mapreduce-reducer-$i"
  else
    echo "Failed to create Redis instance mapreduce-reducer-$i"
    exit 1
  fi

  REDIS_HOST=$(gcloud redis instances describe mapreduce-reducer-"$i" \
                --region=europe-west2 \
                --format="value(host)")
  REDIS_PORT=$(gcloud redis instances describe mapreduce-reducer-"$i" \
                --region=europe-west2 \
                --format="value(port)")

  echo "Deploying reducer $i"
  if (gcloud functions deploy reducer-"$i" \
  		--gen2 \
  		--runtime=go116 \
  		--trigger-topic mapreduce-reducer-"$i" \
  		--source=. \
  		--entry-point Reducer \
  		--region=europe-west2 \
  		--memory=512MB \
      --project=serverless-mapreduce \
      --vpc-connector=projects/serverless-mapreduce/locations/europe-west2/connectors/mapreduce-connector \
      --set-env-vars=REDIS_HOST="$REDIS_HOST",REDIS_PORT="$REDIS_PORT"
      ) ; then
    echo "Successfully deployed reducer $i"
  else
    echo "Failed to deploy reducer $i"
    exit 1
  fi

  # Change the backoff delay of the subscription to start at 1 second
  subscription=$(gcloud pubsub subscriptions list | grep "eventarc-europe-west2-reducer-$i" | cut -c 7-)
  echo "Changing backoff delay of subscription $subscription"
  gcloud pubsub subscriptions update "$subscription" \
    --project=serverless-mapreduce \
    --min-retry-delay=1s \
    --max-retry-delay=10s
  ) &
done; wait