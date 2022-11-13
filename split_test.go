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

func TestSplitter(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-mapper-0")
	defer teardown(t)
	// Given
	// Create a message
	inputData := "1. The quick brown fox jumps over the lazy dog"
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       []byte(inputData),
			Attributes: map[string]string{"splitter": "0"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err := e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := MapperData{
		Text: []string{"quick", "brown", "fox", "jumps", "over", "lazy", "dog"},
	}

	// When
	err = splitter(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult MapperData
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

func TestProcessText(t *testing.T) {
	// Given
	inputText := []byte("1. The quick brown fox jumps over the lazy dog.")
	expectedResult := []string{"quick", "brown", "fox", "jumps", "over", "lazy", "dog"}

	// When
	actualResult := processText(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}
