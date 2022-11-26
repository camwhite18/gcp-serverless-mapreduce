package reduce_phase

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"log"
	"time"
)

func init() {
	functions.CloudEvent("Reducer", reducer)
}

func reducer(ctx context.Context, e event.Event) error {
	start := time.Now()
	//Initialize the redis pool if it hasn't been initialized yet
	if sm.RedisPool == nil {
		var err error
		sm.RedisPool, err = sm.InitRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	var wordDataSlice []sm.WordData
	client, attributes, err := sm.ReadPubSubMessage(context.Background(), e, &wordDataSlice)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	conn := sm.RedisPool.Get()
	defer conn.Close()
	// Store the data in a set in redis
	for _, wordData := range wordDataSlice {
		for word := range wordData.Anagrams {
			_, err := conn.Do("SADD", wordData.SortedWord, word)
			if err != nil {
				log.Printf("error pushing value to set in redis: %v", err)
			}
		}
	}
	// Send a message to the controller that we're done
	// Create a client for the controller topic
	controllerTopic := client.Topic("mapreduce-controller")
	defer controllerTopic.Stop()
	statusMessage := sm.StatusMessage{
		Id:     attributes["partitionId"],
		Status: sm.STATUS_FINISHED,
	}
	sm.SendPubSubMessage(ctx, nil, controllerTopic, statusMessage, attributes)
	log.Printf("sent message to controller about reducer %s, partition %s", attributes["reducerNum"], attributes["partitionId"])
	log.Printf("reducer took %v", time.Since(start))
	return nil
}
