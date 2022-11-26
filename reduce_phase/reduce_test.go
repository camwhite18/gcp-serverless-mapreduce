package reduce_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"testing"
	"time"
)

func TestReducer(t *testing.T) {
	// Given
	teardown, subscriptions := sm.SetupTest(t, []string{"mapreduce-controller"})
	defer teardown(t)
	teardownRedis := sm.SetupRedisTest(t)
	defer teardownRedis(t)
	wordDataSlice := []sm.WordData{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
	}
	// Create a message
	wordDataBytes, err := json.Marshal(wordDataSlice)
	if err != nil {
		t.Fatalf("Error marshalling word data: %v", err)
	}
	message := sm.MessagePublishedData{
		Message: sm.PubSubMessage{
			Data:       wordDataBytes,
			Attributes: map[string]string{"reducerNum": "1"},
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []string{"care", "race"}
	expectedControllerResult := sm.StatusMessage{Status: sm.STATUS_FINISHED}

	// When
	err = reducer(context.Background(), e)

	// Then
	assert.Nil(t, err)
	conn := sm.RedisPool.Get()
	defer conn.Close()

	li, err := conn.Do("SORT", wordDataSlice[0].SortedWord, "ALPHA")
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	for _, v := range li.([]interface{}) {
		actualResult = append(actualResult, string(v.([]byte)))
	}
	assert.Equal(t, expectedResult, actualResult)

	// Ensure the controller received the correct message
	// The subscription will listen forever unless given a context with a timeout
	controllerCtx, controllerCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer controllerCancel()
	var received sm.StatusMessage
	err = subscriptions[0].Receive(controllerCtx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &received)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		assert.Equal(t, msg.Attributes["reducerNum"], "1")
		msg.Ack()
	})
	// Ensure the message data matches the expected result
	assert.Equal(t, expectedControllerResult.Status, received.Status)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}
