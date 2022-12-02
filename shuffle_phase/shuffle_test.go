package shuffle_phase

import (
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"testing"
)

func TestShuffler(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupTest(t, []string{pubsub.CONTROLLER_TOPIC})
	defer teardown(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	// Given
	// Create a message
	inputData := []pubsub.MappedWord{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
		{SortedWord: "aprt", Anagrams: map[string]struct{}{"trap": {}, "part": {}}},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Shuffler data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Shuffler(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// Check that the data was stored in Redis
	result1, err := redis.MultiRedisClient["1"].LRange(context.Background(), "acer", 0, -1).Result()
	if err != nil {
		t.Fatalf("Error getting data from Redis: %v", err)
	}
	result2, err := redis.MultiRedisClient["1"].LRange(context.Background(), "aprt", 0, -1).Result()
	if err != nil {
		t.Fatalf("Error getting data from Redis: %v", err)
	}
	assert.Contains(t, result1, "care")
	assert.Contains(t, result1, "race")
	assert.Contains(t, result2, "part")
	assert.Contains(t, result2, "trap")
}

func TestShuffler_ReadPubSubMessageError(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupTest(t, []string{pubsub.CONTROLLER_TOPIC})
	defer teardown(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	// Given
	// Create a message
	inputData := []int{1, 2, 3}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Shuffler data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Shuffler(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error unmarshalling message:")
}

func TestShuffler_CreatePubSubClientError(t *testing.T) {
	// Setup test
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	// Given
	// Create a message
	inputData := []pubsub.MappedWord{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
		{SortedWord: "aprt", Anagrams: map[string]struct{}{"trap": {}, "part": {}}},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Shuffler data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Shuffler(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating pubsub client:")
}

func TestPartition(t *testing.T) {
	// Given
	inputData := "acer"

	// When
	reducerNum := partitioner(inputData)

	// Then
	assert.Equal(t, 1, reducerNum)
}
