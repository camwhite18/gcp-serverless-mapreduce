# Serverless MapReduce to find Anagrams in a dataset

## Deployment


## Tests

Before running any tests, you will need to run several Docker images that mock GCP cloud services used in the project.
This includes an official image by Google `gcr.io/google.com/cloudsdktool/cloud-sdk:latest` which I use to mock the 
PubSub service, and a open-source image created by _ `oittaa/gcp-storage-emulator` which I use to mock Storage Buckets.

[//]: # (TODO: Find a link to the GitHub)

Unit tests exist for all the functions written in the project. I used these throughout the development of the serverless
MapReduce in order to ensure that no changes I made to my code caused anything to break. To run these unit tests, one 
can run the command:
```shell
make test-unit
```

A script also exists that allows for checking of the test coverage of each package (note that you will need a newer 
version of bash that allows `declare -A`, I installed it using Homebrew). This can be run using the command:
```shell
make test-coverage
```

## Sample Output

A bucket exists in GCP that contains the output files containing all the anagrams from the 100 books as required by the 
22COC105 coursework. In order to download these files, one can run the command:

[//]: # (TODO: Make output bucket accessible from internet (read-only)
```shell
gsutil -m cp -R gs://serverless-mapreduce-output/ .
```