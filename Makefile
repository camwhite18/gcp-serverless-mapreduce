setup-test: create-pubsub-emulator create-local-redis create-storage-emulator

teardown-test: remove-pubsub-emulator remove-local-redis remove-storage-emulator

test-unit:
	./scripts/test-unit.sh

test-coverage:
	./scripts/coverage-report.sh

test: setup-test test-unit test-coverage teardown-test

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
	./reducephase/deploy-reducer.sh

remove-reducer:
	./reducephase/delete-reducer.sh

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

start:
	./scripts/start-anagram-mapreduce.sh

create-pubsub-emulator:
	@docker-compose up -d pubsub-emulator

remove-pubsub-emulator:
	@docker-compose rm -s pubsub-emulator

create-storage-emulator:
	@docker-compose run -dp 9023:9023 storage-emulator

remove-storage-emulator:
	@docker rm --force $$(docker ps | grep -FEo -- "[a-z0-9]+-[a-z0-9_]+storage-emulator[a-z0-9_]+")

create-local-redis:
	@docker-compose up -d redis

remove-local-redis:
	@docker-compose rm -s redis