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

func TestMapper(t *testing.T) {
	// Setup test
	teardown, subscription := setupTest(t, "mapreduce-shuffler-6")
	defer teardown(t)
	// Given
	// Create a message
	inputData := "race"
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data: []byte(inputData),
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := WordData{
		SortedWord: "acer",
		Word:       inputData,
	}

	// When
	err = mapper(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var actualResult WordData
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		// Ensure the message data matches the expected result
		assert.Equal(t, expectedResult, actualResult)
		msg.Ack()
	})
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}
