package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func setupTest(tb testing.TB) (func(tb testing.TB), *pubsub.Subscription) {
	// Setup test
	// Modify the PUBSUB_EMULATOR_HOST environment variable to point to the pubsub emulator
	existingVal := os.Getenv("PUBSUB_EMULATOR_HOST")
	err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a pubsub client so we can create a topic and subscription
	client, err := pubsub.NewClient(context.Background(), "serverless-mapreduce")
	if err != nil {
		tb.Fatalf("Error creating pubsub client: %v", err)
	}
	t, err := client.CreateTopic(context.Background(), "mapreduce-shuffler")
	if err != nil {
		tb.Fatalf("Error creating topic: %v", err)
	}
	subscription, err := client.CreateSubscription(context.Background(), "mapreduce-shuffler", pubsub.SubscriptionConfig{
		Topic: t,
	})
	if err != nil {
		tb.Fatalf("Error creating subscription: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		// Delete the subscription
		if err := subscription.Delete(context.Background()); err != nil {
			tb.Fatalf("Error deleting subscription: %v", err)
		}
		// Delete the topic
		if err := t.Delete(context.Background()); err != nil {
			tb.Fatalf("Error deleting topic: %v", err)
		}
		// Reset the PUBSUB_EMULATOR_HOST environment variable
		err = os.Setenv("PUBSUB_EMULATOR_HOST", existingVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}, subscription
}

func TestMapper(t *testing.T) {
	// Setup test
	teardown, subscription := setupTest(t)
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
