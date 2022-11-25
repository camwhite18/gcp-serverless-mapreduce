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

func TestMapReduceController_StatusStarted(t *testing.T) {
	// Given
	teardown, _ := SetupTest(t, "mapreduce-outputter-0")
	defer teardown(t)
	teardownRedis := SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := StatusMessage{
		Id:         "12345",
		Status:     STATUS_STARTED,
		ReducerNum: "",
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
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
	err = controller(context.Background(), e)

	// Then
	assert.Nil(t, err)
	conn := controllerRedisPool.Get()
	defer conn.Close()

	li, err := conn.Do("SMEMBERS", "started-reducer-0")
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	for _, v := range li.([]interface{}) {
		actualResult = append(actualResult, string(v.([]byte)))
	}
	assert.Equal(t, expectedResult, actualResult)
}

func TestMapReduceController_StatusFinished(t *testing.T) {
	// Given
	teardown, subscription := SetupTest(t, "mapreduce-outputter-0")
	defer teardown(t)
	teardownRedis := SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := StatusMessage{
		Id:         "12345",
		Status:     STATUS_FINISHED,
		ReducerNum: "0",
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
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

	controllerRedisPool, err := initRedisPool()
	if err != nil {
		t.Fatalf("Error initializing redis pool: %v", err)
	}
	conn := controllerRedisPool.Get()
	defer conn.Close()
	_, err = conn.Do("SADD", "started-reducer-0", "12345")

	var expectedResult interface{}
	expectedResult = nil

	// When
	err = controller(context.Background(), e)

	// Then
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult interface{}
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

	cardinality, err := conn.Do("SCARD", "started-reducer-0")
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	assert.Equal(t, int64(0), cardinality)
}
