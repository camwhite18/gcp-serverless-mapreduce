#!/usr/bin/env bash
#
## Check if gcloud is installed
#if ! [ -x "$(command -v gcloud)" ]; then
#  echo 'Error: gcloud is not installed.' >&2
#  exit 1
#fi
#
## Create the topic and deploy the shuffler
#echo "Creating topic mapreduce-shuffler"
#if (gcloud pubsub topics create mapreduce-shuffler \
#  --project=serverless-mapreduce) ; then
#  echo "Successfully created topic mapreduce-shuffler"
#else
#  echo "Failed to create topic mapreduce-shuffler"
#  exit 1
#fi
#
## Create VPC connector for serverless VPC access to Redis
#if (gcloud compute networks vpc-access connectors create mapreduce-connector \
#    --project=serverless-mapreduce \
#    --network=default \
#    --region=europe-west2 \
#    --max-instances=3 \
#    --range=10.8.0.0/28) ; then
#  echo "Successfully created VPC connector"
#else
#  echo "Failed to create VPC connector or it already exists"
#fi
#
num_reducers=5
#for ((i=0;i<num_reducers;i++)) do
#  ( \
#  echo "Creating Redis instance mapreduce-redis-$i"
#  if (gcloud redis instances create mapreduce-redis-"$i" \
#      --tier=basic \
#      --region=europe-west2 \
#      --size=1 \
#      --network=default) ; then
#    echo "Successfully created Redis instance mapreduce-redis-$i"
#  else
#    echo "Failed to create Redis instance mapreduce-redis-$i"
#    exit 1
#  fi
#  ) &
#done; wait

REDIS_HOSTS=$(gcloud redis instances describe mapreduce-redis-0 \
              --region=europe-west2 \
              --format="value(host)")
for ((i=1;i<num_reducers;i++)) do
  REDIS_HOST=$(gcloud redis instances describe mapreduce-redis-"$i" \
                --region=europe-west2 \
                --format="value(host)")
  REDIS_HOSTS+=" $REDIS_HOST"
done
echo "$REDIS_HOSTS"

echo "Deploying shuffler"
if (gcloud functions deploy shuffler \
    --gen2 \
    --runtime=go116 \
    --trigger-topic mapreduce-shuffler \
    --source=. \
    --entry-point Shuffler \
    --region=europe-west2 \
    --memory=512MB \
    --project=serverless-mapreduce \
    --vpc-connector=projects/serverless-mapreduce/locations/europe-west2/connectors/mapreduce-connector \
    --set-env-vars=REDIS_HOSTS="$REDIS_HOSTS"
    ) ; then
  echo "Successfully deployed shuffler"
else
  echo "Failed to deploy shuffler"
fi

# Change the backoff delay of the subscription to start at 1 second
subscription=$(gcloud pubsub subscriptions list | grep "eventarc-europe-west2-shuffler" | cut -c 7-)
echo "Changing backoff delay of subscription $subscription"
gcloud pubsub subscriptions update "$subscription" \
  --project=serverless-mapreduce \
  --min-retry-delay=1s \
  --max-retry-delay=10s
