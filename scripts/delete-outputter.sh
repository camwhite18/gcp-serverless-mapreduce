#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the outputters
num_outputters=5
for ((i=0;i<num_outputters;i++)) do
  ( \
  echo "Deleting topic mapreduce-outputter-$i"
  if (gcloud pubsub topics delete mapreduce-outputter-"$i" \
      --project=serverless-mapreduce) ; then
    echo "Successfully deleted topic mapreduce-outputter-$i"
  else
    echo "Failed to delete topic mapreduce-outputter-$i"
  fi

  echo "Deleting outputter $i"
  if (gcloud functions delete outputter-"$i" \
    --gen2 \
    --region=europe-west2 \
    --project=serverless-mapreduce \
    --quiet) ; then
    echo "Successfully deleted outputter $i"
  else
    echo "Failed to delete outputter $i"
    exit 1
  fi
  ) &
done; wait
