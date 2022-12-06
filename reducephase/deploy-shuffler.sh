#!/usr/bin/env bash

# Read env file
source .env

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Create the topic and deploy the shuffler
echo "Creating topic mapreduce-shuffler"
if (gcloud pubsub topics create mapreduce-shuffler \
  --project="$GCP_PROJECT") ; then
  echo "Successfully created topic mapreduce-shuffler"
else
  echo "Failed to create topic mapreduce-shuffler"
  exit 1
fi

REDIS_HOSTS=$(gcloud redis instances describe mapreduce-redis-0 \
              --region="$GCP_REGION" \
              --format="value(host)")
for ((i=1;i<"$NO_OF_REDUCERS";i++)) do
  REDIS_HOST=$(gcloud redis instances describe mapreduce-redis-"$i" \
                --region="$GCP_REGION" \
                --format="value(host)")
  REDIS_HOSTS+=" $REDIS_HOST"
done

echo "Deploying shuffler"
if (gcloud functions deploy shuffler \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-shuffler \
    --source=. \
    --entry-point Shuffler \
    --region="$GCP_REGION" \
    --memory=512MB \
    --project="$GCP_PROJECT" \
    --vpc-connector=projects/"$GCP_PROJECT"/locations/"$GCP_REGION"/connectors/mapreduce-connector \
    --set-env-vars=REDIS_HOSTS="$REDIS_HOSTS",GCP_PROJECT="$GCP_PROJECT",NO_OF_REDUCERS="$NO_OF_REDUCERS"
    ) ; then
  echo "Successfully deployed shuffler"
else
  echo "Failed to deploy shuffler"
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-$GCP_REGION-shuffler" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project="$GCP_PROJECT" \
  --min-retry-delay=1s \
  --max-retry-delay=10s
