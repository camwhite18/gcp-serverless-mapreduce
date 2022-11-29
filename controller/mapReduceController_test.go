package controller

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

func TestMapReduceController_StatusStarted(t *testing.T) {
	// Given
	teardown, _ := tools.SetupTest(t, []string{tools.OUTPUTTER_TOPIC + "-0"})
	defer teardown(t)
	teardownRedis := tools.SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := tools.StatusMessage{
		Id:     "12345",
		Status: tools.STATUS_STARTED,
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := tools.MessagePublishedData{
		Message: tools.PubSubMessage{
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
	conn := tools.RedisPool.Get()
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
	teardown, subscriptions := tools.SetupTest(t, []string{tools.OUTPUTTER_TOPIC + "-0"})
	defer teardown(t)
	teardownRedis := tools.SetupRedisTest(t)
	defer teardownRedis(t)
	statusMessage := tools.StatusMessage{
		Id:     "12345",
		Status: tools.STATUS_FINISHED,
	}
	// Create a message
	statusMessageBytes, err := json.Marshal(statusMessage)
	if err != nil {
		t.Fatalf("Error marshalling status message: %v", err)
	}
	message := tools.MessagePublishedData{
		Message: tools.PubSubMessage{
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

	controllerRedisPool, err := tools.InitRedisPool()
	if err != nil {
		t.Fatalf("Error initializing redis pool: %v", err)
	}
	conn := controllerRedisPool.Get()
	defer conn.Close()
	_, err = conn.Do("SADD", "started-reducer-0", "12345")

	// When
	err = Controller(context.Background(), e)

	// Then
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		assert.NotNil(t, msg)
		msg.Ack()
	})
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)

	cardinality, err := conn.Do("SCARD", "started-reducer-0")
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	assert.Equal(t, int64(0), cardinality)
}
