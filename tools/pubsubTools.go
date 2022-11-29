package tools

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"sync"
)

// ReadPubSubMessage reads a pubsub message from the given subscription and returns a pubsub client and the attributes
// of the received message.
func ReadPubSubMessage(ctx context.Context, e event.Event, data interface{}) (*pubsub.Client, map[string]string, error) {
	// Get the message from the event data
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return nil, nil, fmt.Errorf("error getting data from event: %v", err)
	}
	// Attempt to unmarshal the message data into the given data interface
	if err := json.Unmarshal(msg.Message.Data, &data); err != nil && data != nil {
		return nil, nil, fmt.Errorf("error unmarshalling message: %v", err)
	}
	// Create a pubsub client
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		return nil, nil, fmt.Errorf("error creating pubsub client: %v", err)
	}
	return client, msg.Message.Attributes, nil
}

// SendPubSubMessage sends a message to the given topic. The message is marshalled into JSON and sent as the data of the
// pubsub message. The attributes are also sent with the message.
func SendPubSubMessage(ctx context.Context, wg *sync.WaitGroup, topic *pubsub.Topic,
	data interface{}, attributes map[string]string) {
	// Since a lot of message are sent concurrently, we need to decrement the wait group when this function returns
	if wg != nil {
		defer wg.Done()
	}
	// Set the topic publish settings
	topic.PublishSettings.ByteThreshold = MAX_MESSAGE_SIZE_BYTES
	topic.PublishSettings.CountThreshold = MAX_MESSAGE_COUNT
	topic.PublishSettings.DelayThreshold = MAX_MESSAGE_DELAY
	// Get the JSON encoding of the data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling word data: %v", err)
		return
	}
	// Push the message to the topic
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       dataBytes,
		Attributes: attributes,
	})
	// Wait for the message to be sent and log any errors
	_, err = result.Get(ctx)
	if err != nil {
		log.Printf("Error publishing message: %v", err)
	}
}

// InitRedisPool creates a redis pool from the given redis address. Uses the REDIS_HOST and REDIS_PORT environment
// variables if they are set.
func InitRedisPool() (*redis.Pool, error) {
	// Get the redis host and port from the environment variables
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		return nil, fmt.Errorf("REDIS_HOST not set")
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		return nil, fmt.Errorf("REDIS_PORT not set")
	}
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)

	// Create a redis pool and return it
	const maxConnections = 10
	return &redis.Pool{
		MaxIdle: maxConnections,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddress)
		},
	}, nil
}
