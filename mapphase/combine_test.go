package mapphase

import (
	ps "cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"testing"
	"time"
)

func TestCombine(t *testing.T) {
	// Setup test
	teardown, subscriptions := test.SetupTest(t, []string{pubsub.ShufflerTopic})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []pubsub.MappedWord{
		{Anagrams: map[string]struct{}{"care": {}}, SortedWord: "acer"},
		{Anagrams: map[string]struct{}{"part": {}}, SortedWord: "artp"},
		{Anagrams: map[string]struct{}{"race": {}}, SortedWord: "acer"},
		{Anagrams: map[string]struct{}{"care": {}}, SortedWord: "acer"},
		{Anagrams: map[string]struct{}{"trap": {}}, SortedWord: "artp"},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []pubsub.MappedWord{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
		{SortedWord: "artp", Anagrams: map[string]struct{}{"part": {}, "trap": {}}},
	}

	// When
	err = Combine(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []pubsub.MappedWord
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *ps.Message) {
		// Unmarshal the message data into the MappedWord struct
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

func TestCombine_ReadPubSubMessageError(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupTest(t, []string{pubsub.ShufflerTopic})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []int{1, 2, 3}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Combine(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error unmarshalling message")
}

func TestCombine_CreatePubSubClientError(t *testing.T) {
	// Given
	// Create a message
	inputData := []int{1, 2, 3}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Combine(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating pubsub client")
}
