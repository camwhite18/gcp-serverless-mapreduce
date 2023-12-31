package controller

import (
	ps "cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"testing"
	"time"
)

func TestMapReduceController_StatusStarted(t *testing.T) {
	// Given
	teardown, _ := test.SetupPubSubTest(t, []string{pubsub.ReducerTopic})
	defer teardown(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := pubsub.ControllerMessage{
		ID:     "12345",
		Status: pubsub.StatusStarted,
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data: statusMessageBytes,
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []string{"12345"}

	// When
	err = Controller(context.Background(), e)

	// Then
	assert.Nil(t, err)

	result := redis.SingleRedisClient.SMembers(context.Background(), "started-processing")
	if result.Err() != nil {
		t.Fatalf("Error getting data from redis: %v", result.Err())
	}
	assert.Equal(t, expectedResult, result.Val())
}

func TestMapReduceController_StatusFinished(t *testing.T) {
	// Given
	teardown, subscriptions := test.SetupPubSubTest(t, []string{pubsub.ReducerTopic})
	defer teardown(t)
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := pubsub.ControllerMessage{
		ID:     "12345",
		Status: pubsub.StatusFinished,
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       statusMessageBytes,
			Attributes: map[string]string{"reducerNum": "0"},
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	redis.SingleRedisClient.SAdd(context.Background(), "started-processing", "12345")

	// When
	err = Controller(context.Background(), e)

	// Then
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *ps.Message) {
		assert.NotNil(t, msg)
		msg.Ack()
	})
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)

	cardinality, err := redis.SingleRedisClient.SCard(context.Background(), "started-processing").Result()
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	assert.Equal(t, int64(0), cardinality)
}

func TestMapReduceController_ReadPubSubMessageError(t *testing.T) {
	// Given
	teardown, _ := test.SetupPubSubTest(t, []string{pubsub.ReducerTopic})
	defer teardown(t)
	statusMessageBytes, err := json.Marshal([]int{1, 2, 3})
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data: statusMessageBytes,
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Controller(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error reading pubsub message")
}

func TestMapReduceController_CreatePubSubClientError(t *testing.T) {
	// Given
	teardownRedis := test.SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := pubsub.ControllerMessage{
		ID:     "12345",
		Status: pubsub.StatusStarted,
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data: statusMessageBytes,
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Controller(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating pubsub client")
}
