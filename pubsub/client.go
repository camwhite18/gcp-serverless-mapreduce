package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
	"os"
)

// Client is an interface for interacting with pubsub.
type Client interface {
	Close()
	ReadPubSubMessage(data interface{}) (map[string]string, error)
	SendPubSubMessage(topicName string, data interface{}, attributes map[string]string)
}

type clientImpl struct {
	ctx    context.Context
	event  event.Event
	client *pubsub.Client
}

var _ Client = &clientImpl{}

// New returns a new pubsub client.
func New(ctx context.Context, e event.Event) (Client, error) {
	// Create a pubsub client
	client, err := pubsub.NewClient(ctx, os.Getenv("GCP_PROJECT"))
	if err != nil {
		return nil, fmt.Errorf("error creating pubsub client: %v", err)
	}
	return &clientImpl{
		ctx:    ctx,
		event:  e,
		client: client,
	}, nil
}

// Close closes the pubsub client.
func (c clientImpl) Close() {
	err := c.client.Close()
	if err != nil {
		log.Printf("Error closing pubsub client: %v", err)
	}
}

// ReadPubSubMessage reads a pubsub message from the given subscription and returns a pubsub client and the attributes
// of the received message.
func (c clientImpl) ReadPubSubMessage(data interface{}) (map[string]string, error) {
	// Get the message from the event data
	var msg MessagePublishedData
	if err := c.event.DataAs(&msg); err != nil {
		return nil, fmt.Errorf("error getting data from event: %v", err)
	}
	// Attempt to unmarshal the message data into the given data interface
	if err := json.Unmarshal(msg.Message.Data, &data); err != nil && data != nil {
		return nil, fmt.Errorf("error unmarshalling message: %v", err)
	}
	return msg.Message.Attributes, nil
}

// SendPubSubMessage sends a message to the given topic. The message is marshalled into JSON and sent as the data of the
// pubsub message. The attributes are also sent with the message.
func (c clientImpl) SendPubSubMessage(topicName string, data interface{}, attributes map[string]string) {
	// Get the JSON encoding of the data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling word data: %v", err)
		return
	}
	// Create a topic to send messages to
	topic := c.client.Topic(topicName)
	defer topic.Stop()
	// Set the topic publish settings
	topic.PublishSettings.ByteThreshold = MaxMessageSizeBytes
	topic.PublishSettings.CountThreshold = MaxMessageCount
	topic.PublishSettings.DelayThreshold = MaxMessageDelay
	// Push the message to the topic
	result := topic.Publish(c.ctx, &pubsub.Message{
		Data:       dataBytes,
		Attributes: attributes,
	})
	// Wait for the message to be sent and log any errors
	_, err = result.Get(c.ctx)
	if err != nil {
		log.Printf("Error publishing message: %v", err)
	}
}
