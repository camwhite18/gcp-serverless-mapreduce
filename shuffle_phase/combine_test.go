package shuffle_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"testing"
	"time"
)

func TestCombine(t *testing.T) {
	// Setup test
	teardown, subscriptions := tools.SetupTest(t, []string{"mapreduce-Shuffler"})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []tools.WordData{
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
	message := tools.MessagePublishedData{
		Message: tools.PubSubMessage{
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

	expectedResult := []tools.WordData{
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
	var actualResult []tools.WordData
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
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
