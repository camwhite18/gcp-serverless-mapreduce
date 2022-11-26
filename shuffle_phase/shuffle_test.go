package shuffle_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce"
	"testing"
	"time"
)

func TestShuffler(t *testing.T) {
	// Setup test
	teardown, subscriptions := serverless_mapreduce.SetupTest(t, []string{"mapreduce-reducer-1"})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []serverless_mapreduce.WordData{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling shuffler data: %v", err)
	}
	message := serverless_mapreduce.MessagePublishedData{
		Message: serverless_mapreduce.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []serverless_mapreduce.WordData{
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
	var actualResult []serverless_mapreduce.WordData
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		assert.Equal(t, msg.Attributes["reducerNum"], "1")
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
