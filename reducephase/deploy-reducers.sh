#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topics, redis instances and deploy the mappers
echo "Creating topic mapreduce-reducer"
if (gcloud pubsub topics create mapreduce-reducer \
    --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-reducer"
else
  echo "Failed to create topic mapreduce-reducer"
  exit 1
fi

num_reducers=5
REDIS_HOSTS=$(gcloud redis instances describe mapreduce-redis-0 \
               --region="$GCP_REGION" \
               --format="value(host)")
for ((i=1;i<num_reducers;i++)) do
  REDIS_HOST=$(gcloud redis instances describe mapreduce-redis-"$i" \
                 --region="$GCP_REGION" \
                 --format="value(host)")
  REDIS_HOSTS+=" $REDIS_HOST"
done

echo "Deploying reducer"
if (gcloud functions deploy reducer \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-reducer \
    --source=. \
    --entry-point Reducer \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT" \
    --vpc-connector=projects/"$GCP_PROJECT"/locations/"$GCP_REGION"/connectors/mapreduce-connector \
    --set-env-vars=REDIS_HOSTS="$REDIS_HOSTS"
    ) ; then
  echo "Successfully deployed reducer"
else
  echo "Failed to deploy reducer"
  exit 1
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-reducer" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s