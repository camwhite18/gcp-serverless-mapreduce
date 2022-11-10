#!/bin/bash

# Check if gcloud is installed
if ! [ -x "$(command -v gcloud)" ]; then
  echo 'Error: gcloud is not installed.' >&2
  exit 1
fi

# Delete the splitters
num_splitters=10
for ((i=0;i<num_splitters;i++)) do
  echo "Deleting topic mapreduce-splitter-$i"
  if (gcloud pubsub topics delete mapreduce-splitter-"$i" \
    --project=serverless-mapreduce) ; then
    echo "Successfully deleted topic mapreduce-splitter-$i"
  else
    echo "Failed to delete topic mapreduce-splitter-$i"
  fi

  echo "Deleting splitter $i"
  if (gcloud functions delete splitter-"$i" \
    --gen2 \
    --region=europe-west2 \
    --project=serverless-mapreduce) ; then
    echo "Successfully deleted splitter $i"
  else
    echo "Failed to delete splitter $i"
  fi
done