package pubsub

import (
	ps "cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"testing"
	"time"
)

func TestReadPubSubMessage(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupPubSubTest(t, []string{})
	defer teardown(t)

	// Given
	inputDataBytes, err := json.Marshal([]string{"some", "data"})
	if err != nil {
		t.Fatalf("Error marshalling data: %v", err)
	}
	message := MessagePublishedData{
		Message: Message{
			Data:       inputDataBytes,
			Attributes: map[string]string{"some-key": "some-value"},
		},
	}
	// Create a CloudEvent to be sent to the Mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	client, err := New(context.Background(), e)
	if err != nil {
		t.Fatalf("Error creating pubsub client: %v", err)
	}
	defer client.Close()

	// When
	var data []string
	attr, err := client.ReadPubSubMessage(&data)

	// Then
	assert.Nil(t, err)
	assert.Equal(t, []string{"some", "data"}, data)
	assert.Equal(t, "some-value", attr["some-key"])
}

func TestReadPubSubMessage_UnmarshalDataError(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupPubSubTest(t, []string{})
	defer teardown(t)

	// Given
	inputDataBytes, err := json.Marshal([]int{1, 2, 3})
	if err != nil {
		t.Fatalf("Error marshalling data: %v", err)
	}
	message := MessagePublishedData{
		Message: Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	client, err := New(context.Background(), e)
	if err != nil {
		t.Fatalf("Error creating pubsub client: %v", err)
	}
	defer client.Close()

	// When
	var data []string
	attr, err := client.ReadPubSubMessage(&data)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error unmarshalling message")
	assert.Nil(t, attr)
}

func TestSendPubSubMessage(t *testing.T) {
	// Setup test
	teardown, subscriptions := test.SetupPubSubTest(t, []string{"some-topic"})
	defer teardown(t)

	// Given
	data := []string{"some", "data"}
	attributes := map[string]string{"some-key": "some-value"}

	client, err := New(context.Background(), event.New())
	if err != nil {
		t.Fatalf("Error creating pubsub client: %v", err)
	}
	defer client.Close()

	// When
	client.SendPubSubMessage("some-topic", data, attributes)

	// Then
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []string
	var actualAttr map[string]string
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *ps.Message) {
		// Unmarshal the message data into the MappedWord struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		actualAttr = msg.Attributes
		msg.Ack()
	})
	assert.Nil(t, err)
	assert.Equal(t, []string{"some", "data"}, actualResult)
	assert.Equal(t, "some-value", actualAttr["some-key"])
}
