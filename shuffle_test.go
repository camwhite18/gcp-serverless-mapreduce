package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShuffler(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-reducer-1")
	defer teardown(t)
	// Given
	// Create a message
	inputData := []WordData{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling shuffler data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data: inputDataBytes,
		},
	}
	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []WordData{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
	}

	// When
	err = shuffler(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []WordData
	err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		msg.Ack()
	})
	// Ensure the message data matches the expected result
	assert.Equal(t, expectedResult, actualResult)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}

func TestPartition(t *testing.T) {
	// Given
	inputData := "acer"

	// When
	reducerNum := partition(inputData)

	// Then
	assert.Equal(t, 1, reducerNum)
}
