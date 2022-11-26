package controller

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"log"
	"strconv"
)

func init() {
	functions.CloudEvent("Controller", controller)
}

func controller(ctx context.Context, e event.Event) error {
	//Initialize the controller redis pool if it hasn't been initialized yet
	if sm.RedisPool == nil {
		var err error
		sm.RedisPool, err = sm.InitRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	// Read in id and status from the event
	var statusMessage sm.StatusMessage
	client, attributes, err := sm.ReadPubSubMessage(context.Background(), e, &statusMessage)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	conn := sm.RedisPool.Get()
	defer conn.Close()
	// If the status is "started", then we need to add the id to the "started" set in redis
	if statusMessage.Status == sm.STATUS_STARTED {
		for i := 0; i < sm.NO_OF_REDUCER_INSTANCES; i++ {
			_, err := conn.Do("SADD", "started-reducer-"+strconv.Itoa(i), statusMessage.Id)
			if err != nil {
				return fmt.Errorf("error pushing value to set in redis: %v", err)
			}
			log.Printf("added %v to started-reducer-%v", statusMessage.Id, i)
		}
	} else if statusMessage.Status == sm.STATUS_FINISHED {
		log.Printf("received message from reducer %s, partition %s", attributes["reducerNum"], attributes["partitionId"])
		// If the status is "finished", then we need to remove the id from the "started" set in redis
		_, err := conn.Do("SREM", "started-reducer-"+attributes["reducerNum"], statusMessage.Id)
		if err != nil {
			return fmt.Errorf("error removing value from set in redis: %v", err)
		}
		// Check if the "started" set is empty
		cardinality, err := conn.Do("SCARD", "started-reducer-"+attributes["reducerNum"])
		if err != nil {
			return fmt.Errorf("error checking if set is empty: %v", err)
		}
		log.Printf("cardinality: %v", cardinality)
		if cardinality == int64(0) {
			// If the set is empty, then we need to send a message to start generating the output files
			// Create a client for the controller topic
			outputterTopic := client.Topic("mapreduce-outputter-" + attributes["reducerNum"])
			defer outputterTopic.Stop()
			sm.SendPubSubMessage(ctx, nil, outputterTopic, nil, attributes)
		}
	}
	return nil
}
