package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
	"sync"
)

func ReadPubSubMessage(ctx context.Context, e event.Event, data interface{}) (*pubsub.Client, map[string]string, error) {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return nil, nil, fmt.Errorf("error getting data from event: %v", err)
	}
	if err := json.Unmarshal(msg.Message.Data, &data); err != nil && data != nil {
		return nil, nil, fmt.Errorf("error unmarshalling message: %v", err)
	}
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		return nil, nil, fmt.Errorf("error creating pubsub client: %v", err)
	}
	return client, msg.Message.Attributes, nil
}

func SendPubSubMessage(ctx context.Context, wg *sync.WaitGroup, topic *pubsub.Topic,
	data interface{}, attributes map[string]string) {
	if wg != nil {
		defer wg.Done()
	}
	topic.PublishSettings.ByteThreshold = MAX_MESSAGE_SIZE_BYTES
	topic.PublishSettings.CountThreshold = MAX_MESSAGE_COUNT
	topic.PublishSettings.DelayThreshold = MAX_MESSAGE_DELAY
	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling word data: %v", err)
		return
	}
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       dataBytes,
		Attributes: attributes,
	})
	_, err = result.Get(ctx)
	if err != nil {
		log.Printf("Error publishing message: %v", err)
	}
}
