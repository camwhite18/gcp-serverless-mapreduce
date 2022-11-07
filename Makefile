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
	@gcloud pubsub topics create mapreduce-mapper
	@gcloud pubsub topics create mapreduce-shuffler
	@gcloud pubsub topics create mapreduce-reducer


deploy-splitter:
	@gcloud functions deploy splitter \
		--gen2 \
		--runtime=go116 \
		--trigger-event-filters="bucket=$(INPUT_BUCKET_NAME)" \
		--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
		--source=. \
		--entry-point=Splitter \
		--region=$(GCP_REGION)

remove-splitter:
	@gcloud functions delete splitter --region=$(GCP_REGION) --gen2

deploy-mapper:
	@gcloud functions deploy mapper \
		--gen2 \
		--runtime go116 \
		--trigger-topic mapreduce-mapper \
		--source=. \
		--entry-point Mapper \
		--region $(GCP_REGION)

remove-mapper:
	@gcloud functions delete mapper --region=$(GCP_REGION) --gen2

deploy-shuffler:
	@gcloud functions deploy shuffler \
		--gen2 \
		--runtime go116 \
		--trigger-topic mapreduce-shuffler \
		--source=. \
		--entry-point Shuffler \
		--region $(GCP_REGION)

remove-shuffler:
	@gcloud functions delete shuffler --region=$(GCP_REGION) --gen2

deploy-reducer:
	@gcloud functions deploy reducer \
		--gen2 \
		--runtime go116 \
		--trigger-topic mapreduce-reducer \
		--source=. \
		--entry-point Reducer \
		--region $(GCP_REGION)

remove-reducer:
	@gcloud functions delete reducer --region=$(GCP_REGION) --gen2

deploy: create-input-bucket create-output-bucket create-topics deploy-splitter deploy-mapper deploy-shuffler deploy-reducer

remove: remove-splitter remove-mapper remove-shuffler remove-reducer

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator