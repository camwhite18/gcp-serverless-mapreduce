#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

docker build -t gcr.io/serverless-mapreduce/shuffler:latest -f shuffle/Dockerfile .
docker push gcr.io/serverless-mapreduce/shuffler:latest
# Create the topics and deploy the shufflers
num_shufflers=2
for ((i=0;i<num_shufflers;i++)) do
  echo "Creating topic mapreduce-shuffler-$i"
  if (gcloud pubsub topics create mapreduce-shuffler-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully created topic mapreduce-shuffler-$i"
  else
    echo "Failed to create topic mapreduce-shuffler-$i"
    exit 1
  fi

  echo "Deploying shuffler $i"
  serviceUrl=$(gcloud run deploy shuffler-"$i" \
      --image=gcr.io/serverless-mapreduce/shuffler:latest \
      --region=europe-west2 \
      --project=serverless-mapreduce \
      --no-allow-unauthenticated | grep -Eo "https://[a-z0-9\.\-]*")

  echo "Creating subscription mapreduce-shuffler-$i-subscription"
  gcloud pubsub subscriptions create mapreduce-shuffler-"$i"-subscription \
    --topic=mapreduce-shuffler-"$i" \
    --ack-deadline=600 \
    --project=serverless-mapreduce \
    --push-endpoint="$serviceUrl" \
    --push-auth-service-account=mapreduce-pubsub@serverless-mapreduce.iam.gserviceaccount.com
done