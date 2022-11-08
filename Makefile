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

create-topics:
	@gcloud pubsub topics create mapreduce-splitter-0
	@gcloud pubsub topics create mapreduce-mapper
	@gcloud pubsub topics create mapreduce-shuffler
	@gcloud pubsub topics create mapreduce-reducer

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
	@gcloud functions deploy splitter-0 \
		--gen2 \
		--runtime=go116 \
		--trigger-topic mapreduce-splitter-0 \
		--source=. \
		--entry-point Splitter \
		--region=$(GCP_REGION) \
        --project=$(GCP_PROJECT)

remove-splitter:
	@gcloud functions delete splitter-0 --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

deploy-mapper:
	@gcloud functions deploy mapper \
		--gen2 \
		--runtime go116 \
		--trigger-topic mapreduce-mapper \
		--source=. \
		--entry-point Mapper \
		--region $(GCP_REGION) \
        --project=$(GCP_PROJECT)

remove-mapper:
	@gcloud functions delete mapper --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

#deploy-shuffler:
#	@gcloud functions deploy shuffler \
#		--gen2 \
#		--runtime go116 \
#		--trigger-topic mapreduce-shuffler \
#		--source=. \
#		--entry-point Shuffler \
#		--region $(GCP_REGION) \
#		--project=$(GCP_PROJECT)
deply-shuffler:
	@gcloud dataflow

remove-shuffler:
	@gcloud functions delete shuffler --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

deploy-reducer:
	@gcloud functions deploy reducer \
		--gen2 \
		--runtime go116 \
		--trigger-topic mapreduce-reducer \
		--source=. \
		--entry-point Reducer \
		--region $(GCP_REGION) \
        --project=$(GCP_PROJECT)

remove-reducer:
	@gcloud functions delete reducer --region=$(GCP_REGION) --project=$(GCP_PROJECT) --gen2

deploy: create-input-bucket create-output-bucket create-topics deploy-splitter deploy-mapper deploy-shuffler deploy-reducer

remove: remove-splitter remove-mapper remove-shuffler remove-reducer

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator

create-storage-emulator:
	@docker-compose run -dp 9023:9023 storage-emulator

remove-storage-emulator:
	@docker-compose rm -s storage-emulator