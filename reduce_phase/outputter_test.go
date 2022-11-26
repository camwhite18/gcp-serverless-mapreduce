package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"io"
	"testing"
	"time"
)

func TestOutputResult(t *testing.T) {
	// Setup test
	teardown, _ := sm.SetupTest(t, []string{})
	defer teardown(t)
	teardownStorage := sm.CreateTestStorage(t)
	defer teardownStorage(t)
	teardownRedis := sm.SetupRedisTest(t)
	defer teardownRedis(t)
	// Given
	// Create a message
	message := sm.MessagePublishedData{
		Message: sm.PubSubMessage{
			Attributes: map[string]string{"reducerNum": "1", "outputBucket": sm.OUTPUT_BUCKET_NAME},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// Add data to redis
	conn := sm.RedisPool.Get()
	defer conn.Close()
	_, err = conn.Do("SADD", "acer", "race")
	if err != nil {
		t.Fatalf("Error adding data to redis: %v", err)
	}
	_, err = conn.Do("SADD", "acer", "care")
	if err != nil {
		t.Fatalf("Error adding data to redis: %v", err)
	}

	expectedResult := []byte("acer: care race\n")

	// When
	err = outputResult(context.Background(), e)

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
	outputBucket := client.Bucket(sm.OUTPUT_BUCKET_NAME)
	// Create a reader to read the file
	reader, err := outputBucket.Object("reducer-1-output.txt").NewReader(storageCtx)
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}
	// Read the file
	actualResult, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	// Check that the data is correct
	assert.Equal(t, expectedResult, actualResult)
}
