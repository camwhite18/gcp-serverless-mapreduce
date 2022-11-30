package shuffle_phase

import (
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"testing"
)

func TestShuffler(t *testing.T) {
	// Setup test
	teardown, _ := tools.SetupTest(t, []string{pubsub.CONTROLLER_TOPIC})
	defer teardown(t)
	teardownRedis := tools.SetupRedisTest(t)
	defer teardownRedis(t)
	// Given
	// Create a message
	inputData := []pubsub.MapperData{
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

}

func TestPartition(t *testing.T) {
	// Given
	inputData := "acer"

	// When
	reducerNum := partition(inputData)

	// Then
	assert.Equal(t, 1, reducerNum)
}
