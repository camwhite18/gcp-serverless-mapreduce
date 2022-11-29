package reduce_phase

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"log"
	"time"
)

// Reducer is a function that is triggered by a message being published to the Reducer topic. It receives the list of
// key-value pairs from the shuffler, and inserts each key value pair into the Reducer's Redis instance. It requires the
// message data to be of type []WordData.
func Reducer(ctx context.Context, e event.Event) error {
	start := time.Now()
	//Initialize the redis pool if it hasn't been initialized yet
	if tools.RedisPool == nil {
		var err error
		tools.RedisPool, err = tools.InitRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}
	// Read the data from the event i.e. message pushed from shuffler
	var wordDataSlice []tools.WordData
	client, attributes, err := tools.ReadPubSubMessage(context.Background(), e, &wordDataSlice)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	defer client.Close()
	// Get a connection from the redis pool
	conn := tools.RedisPool.Get()
	defer conn.Close()
	// Loop through the word data and insert each key value pair into the redis instance
	for _, wordData := range wordDataSlice {
		for word := range wordData.Anagrams {
			// Insert the key value pair into the redis instance, where the key is the sorted word and the value is added
			// to a set of anagrams
			_, err := conn.Do("SADD", wordData.SortedWord, word)
			if err != nil {
				return fmt.Errorf("error pushing value to set in redis: %v", err)
			}
		}
	}
	// Create a client for the controller topic
	controllerTopic := client.Topic(tools.CONTROLLER_TOPIC)
	defer controllerTopic.Stop()
	statusMessage := tools.StatusMessage{
		Id:     attributes["partitionId"],
		Status: tools.STATUS_FINISHED,
	}
	// Send a message to the controller that the given partition has finished being processed
	tools.SendPubSubMessage(ctx, nil, controllerTopic, statusMessage, attributes)
	log.Printf("Reducer took %v", time.Since(start))
	return nil
}
