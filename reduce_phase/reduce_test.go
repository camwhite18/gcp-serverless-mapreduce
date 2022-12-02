package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"io"
	"testing"
	"time"
)

func TestReducer(t *testing.T) {
	// Given
	teardown, _ := test.SetupTest(t, []string{})
	defer teardown(t)
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)

	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Attributes: map[string]string{"outputBucket": test.OUTPUT_BUCKET_NAME, "redisNum": "1"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	redis.MultiRedisClient["1"].LPush(context.Background(), "acer", "race", "race", "care", "race")
	redis.MultiRedisClient["1"].LPush(context.Background(), "aprt", "part", "trap", "trap", "part")

	expectedResult1 := "acer: care race\n"
	expectedResult2 := "aprt: part trap\n"

	// When
	err = Reducer(context.Background(), e)

	// Then
	assert.Nil(t, err)
	// Check that the data was stored in the file correctly
	storageCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	// Create a storage client so we can create a bucket
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	// Create a reader to read the file
	reader, err := client.Bucket(test.OUTPUT_BUCKET_NAME).Object("anagrams-part-1.txt").NewReader(storageCtx)
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}
	// Read the file
	actualResult, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	// Check that the data is correct
	assert.Contains(t, string(actualResult), expectedResult1)
	assert.Contains(t, string(actualResult), expectedResult2)
}

func TestReducer_CreateStorageClientWithWriterError(t *testing.T) {
	// Given
	teardown, _ := test.SetupTest(t, []string{})
	defer teardown(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)

	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Attributes: map[string]string{"outputBucket": test.OUTPUT_BUCKET_NAME, "redisNum": "1"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Reducer(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating storage client:")
}

func TestReducer_CreatePubSubClientError(t *testing.T) {
	// Given
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)

	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Attributes: map[string]string{"outputBucket": test.OUTPUT_BUCKET_NAME, "redisNum": "1"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Reducer(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating pubsub client")
}
