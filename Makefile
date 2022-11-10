INPUT_BUCKET_NAME=serverless-mapreduce-input
OUTPUT_BUCKET_NAME=serverless-mapreduce-output
GCP_REGION=europe-west2
GCP_PROJECT=serverless-mapreduce

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

deploy-start-mapreduce:
	@gcloud functions deploy start-mapreduce \
		--gen2 \
		--runtime=go116 \
		--trigger-http \
		--source=. \
		--entry-point StartMapreduce \
		--region=$(GCP_REGION) \
		--project=$(GCP_PROJECT)

remove-start-mapreduce:
	@gcloud functions delete start-mapreduce --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

deploy-splitter:
	./scripts/deploy-splitters.sh

remove-splitter:
	./scripts/delete-splitters.sh

deploy-mapper:
	./scripts/deploy-mappers.sh

remove-mapper:
	./scripts/delete-mappers.sh

#deploy-shuffler:
#	./scripts/deploy-shufflers.sh
deploy-shuffler:
	@gcloud dataflow

remove-shuffler:
	./scripts/delete-shufflers.sh

deploy-reducer:
	./scripts/deploy-reducers.sh

remove-reducer:
	./scripts/delete-reducers.sh

deploy: create-input-bucket create-output-bucket deploy-splitter deploy-mapper deploy-shuffler deploy-reducer

remove: remove-splitter remove-mapper remove-shuffler remove-reducer

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator

create-storage-emulator:
	@docker-compose run -dp 9023:9023 storage-emulator

remove-storage-emulator:
	@docker-compose rm -s storage-emulator