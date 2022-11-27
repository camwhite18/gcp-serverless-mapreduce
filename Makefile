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

deploy-controller:
	./scripts/deploy-controller.sh

remove-controller:
	./scripts/remove-controller.sh

deploy-start-mapreduce:
	@gcloud functions deploy init-mapreduce \
		--gen2 \
		--runtime=go116 \
		--trigger-http \
		--source=. \
		--entry-point Service \
		--region=$(GCP_REGION) \
		--memory=512MB \
		--project=$(GCP_PROJECT)

remove-start-mapreduce:
	@gcloud functions delete init --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

deploy-splitter:
	./map_phase/deploy-splitter.sh

remove-splitter:
	./map_phase/delete-splitter.sh

deploy-mapper:
	./map_phase/deploy-mapper.sh

remove-mapper:
	./map_phase/delete-mapper.sh

deploy-combine:
	./shuffle_phase/deploy-combine.sh

remove-combine:
	./shuffle_phase/delete-combine.sh

deploy-shuffler:
	./shuffle_phase/deploy-shuffler.sh

remove-shuffler:
	./shuffle_phase/delete-shuffler.sh

deploy-reducer:
	./reduce_phase/deploy-reducers.sh

remove-reducer:
	./reduce_phase/delete-reducers.sh

deploy-outputter:
	./reduce_phase/deploy-outputter.sh

remove-outputter:
	./reduce_phase/delete-outputter.sh

deploy: deploy-controller \
		deploy-start-mapreduce \
		deploy-splitter \
		deploy-mapper \
		deploy-combine \
		deploy-shuffler \
		deploy-reducer \
		deploy-outputter

remove: remove-controller \
		remove-start-mapreduce \
		remove-splitter \
		remove-mapper \
		remove-combine \
		remove-shuffler \
		remove-reducer \
		remove-outputter

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator

create-storage-emulator:
	@docker-compose run -dp 9023:9023 storage-emulator

remove-storage-emulator:
	@docker-compose rm -s storage-emulator