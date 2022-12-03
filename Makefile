INPUT_BUCKET_NAME=serverless-mapreduce-input
OUTPUT_BUCKET_NAME=serverless-mapreduce-output
GCP_REGION=europe-west2
GCP_PROJECT=serverless-mapreduce

setup-test: create-pubsub-emulator create-storage-emulator create-local-redis

teardown-test: remove-pubsub-emulator remove-storage-emulator remove-local-redis

test-unit:
	./scripts/test-unit.sh

test-coverage:
	./scripts/coverage-report.sh

test: setup-test test-unit test-coverage teardown-test

create-input-bucket:
	@gsutil mb -l $(GCP_REGION) gs://$(INPUT_BUCKET_NAME)

upload-books:
	@gsutil -m cp -r coc105-gutenburg-5000books gs://$(INPUT_BUCKET_NAME)

create-output-bucket:
	@gsutil mb -l $(GCP_REGION) gs://$(OUTPUT_BUCKET_NAME)

create-api-gateway:
	@gcloud api-gateway api-configs create mapreduce-api \
		--api=mapreduce-api \
		--openapi-spec=openapi.yaml \
		--project=$(GCP_PROJECT) \
		--backend-auth-service-account=mapreduce-api@$(GCP_PROJECT).iam.gserviceaccount.com
	@gcloud api-gateway gateways create mapreduce-gateway \
		--api=mapreduce-api \
		--api-config=mapreduce-api \
		--location=$(GCP_REGION) \
		--project=$(GCP_PROJECT)

remove-api-gateway:
	@gcloud api-gateway gateways delete mapreduce-gateway \
		--location=$(GCP_REGION) \
		--project=$(GCP_PROJECT)
	@gcloud api-gateway api-configs delete mapreduce-api \
		--api=mapreduce-api \
		--project=$(GCP_PROJECT)

create-redis:
	./scripts/create-redis.sh

remove-redis:
	./scripts/delete-redis.sh

deploy-controller:
	./controller/deploy-controller.sh

remove-controller:
	./controller/delete-controller.sh

deploy-starter:
	./mapphase/deploy-starter.sh

remove-starter:
	./mapphase/delete-starter.sh

deploy-splitter:
	./mapphase/deploy-splitter.sh

remove-splitter:
	./mapphase/delete-splitter.sh

deploy-mapper:
	./mapphase/deploy-mapper.sh

remove-mapper:
	./mapphase/delete-mapper.sh

deploy-combiner:
	./mapphase/deploy-combiner.sh

remove-combiner:
	./mapphase/delete-combiner.sh

deploy-shuffler:
	./reducephase/deploy-shuffler.sh

remove-shuffler:
	./reducephase/delete-shuffler.sh

deploy-reducer:
	./reducephase/deploy-reducers.sh

remove-reducer:
	./reducephase/delete-reducers.sh

deploy: create-redis \
		deploy-controller \
		deploy-starter \
		deploy-splitter \
		deploy-mapper \
		deploy-combiner \
		deploy-shuffler \
		deploy-reducer \

remove: remove-redis \
		remove-controller \
		remove-starter \
		remove-splitter \
		remove-mapper \
		remove-combiner \
		remove-shuffler \
		remove-reducer \

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator

create-storage-emulator:
	@docker-compose run -dp 9023:9023 storage-emulator

remove-storage-emulator:
	@docker-compose rm -s storage-emulator

create-local-redis:
	@docker-compose up -d redis

remove-local-redis:
	@docker-compose rm -s redis