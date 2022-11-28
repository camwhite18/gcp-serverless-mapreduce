# Serverless MapReduce to find Anagrams in a dataset

### Introduction


### Prerequisites

- In order to deploy any part of the project, you need to have gcloud CLI installed and configured. You can find the 
instructions [here](https://cloud.google.com/sdk/docs/quickstarts).
- You need to have a GCP project with billing enabled. You can find the instructions 
[here](https://cloud.google.com/billing/docs/how-to/modify-project).
- In order to run the tests, you need to have Go version 1.16 installed. You can find the instructions
[here](https://golang.org/doc/install).

### Deployment

Deploying the functions is extremely easy due to the Bash scripts provided in each directory. To run these scripts, Make
commands are provided. You can find the Makefile in the root directory of the project. The commands are:

```bash
# Deploy all the functions
make deploy

# OR individually

# Deploy the controller function
make deploy-controller
# Deploy the starter function
make deploy-init-mapreduce
# Deploy the splitter function
make deploy-splitter
# Deploy the mapper function
make deploy-mapper
# Deploy the combine function
make deploy-combine
# Deploy the shuffler function
make deploy-shuffler
# Deploy the reducer function
make deploy-reducer
# Deploy the outputter function
make deploy-outputter
```

Similarly, you can delete the functions using the following commands:

```bash
# Delete all the functions
make remove

# OR individually

# Delete the controller function
make remove-controller
# Delete the starter function
make remove-init-mapreduce
# Delete the splitter function
make remove-splitter
# Delete the mapper function
make remove-mapper
# Delete the combine function
make remove-combine
# Delete the shuffler function
make remove-shuffler
# Delete the reducer function
make remove-reducer
# Delete the outputter function
make remove-outputter
```

**Note:** The deployment scripts are written in Bash and were tested on Linux and macOS. They may not work on Windows.

**Warning:** The deployment scripts create several services in GCP that you will be charged monthly for - namely Redis 
instances and a serverless VPC connector. Make sure you delete these after you are done running the project.

### Starting the MapReduce

To start the MapReduce running on a dataset, you need to ensure that the data is in a Google Cloud Storage bucket. 

[//]: # (TODO: Add instructions on how to start the MapReduce once API gateway is implemented)

### Tests

Before running any tests, you will need to run several Docker images that mock GCP cloud services used in the project.
This includes an official image by Google `gcr.io/google.com/cloudsdktool/cloud-sdk:latest` which I use to mock the 
PubSub service, and a open-source image `oittaa/gcp-storage-emulator` found 
[here](https://hub.docker.com/r/oittaa/gcp-storage-emulator) to mock Storage Buckets. I also use the official Redis 
image `redis-stack:latest` to create a local Redis instance running in a Docker container.

In order to run these, you will need to have Docker installed on your machine. You can find instructions on how to do
this [here](https://docs.docker.com/get-docker/). Once you have Docker installed, you can run the following command to 
create the containers:

```bash
make setup-test
```

Or alternatively, you can run a subset of the containers by running any of the following commands:

```bash
make create-pubsub-emulator 
make create-storage-emulator 
make create-local-redis
```

To remove the containers, you can run the following command:

```bash
make teardown-test
```

Or alternatively, you can remove a subset of the containers by running any of the following commands:

```bash
make remove-pubsub-emulator 
make remove-storage-emulator 
make remove-local-redis
```

Unit tests exist for all the functions written in the project that mock the actual functionality of there use in GCP. 
I used these throughout the development of the MapReduce in order to ensure that no changes I made to my code caused 
anything to break. To run these unit tests, one 
can run the command:
```bash
make test-unit
```

A script also exists that allows for checking of the test coverage of each package (note that you will need a newer 
version of bash that allows `declare -A`, I installed it using Homebrew). This can be run using the command:
```bash
make test-coverage
```

**All of these tests, including the creation and removal of the Docker containers, can be run using the command:**
```bash
make test
```

### Sample Output

A bucket exists in GCP that contains the output files containing all the anagrams from the 100 books as required by the 
22COC105 coursework. The bucket is accessible from the internet read-only. Therefore, in order to download these files, 
one can run the command:

```bash
gsutil -m cp -R gs://serverless-mapreduce-output/ .
```