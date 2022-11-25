package serverless_mapreduce

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

var controllerRedisPool *redis.Pool

func init() {
	functions.CloudEvent("Controller", controller)
}

func controller(ctx context.Context, e event.Event) error {
	//Initialize the controller redis pool if it hasn't been initialized yet
	if controllerRedisPool == nil {
		var err error
		controllerRedisPool, err = initRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	// Read in id and status from the event
	var statusMessage StatusMessage
	client, _, err := ReadPubSubMessage(context.Background(), e, &statusMessage)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	conn := controllerRedisPool.Get()
	defer conn.Close()
	// If the status is "started", then we need to add the id to the "started" set in redis
	if statusMessage.Status == STATUS_STARTED {
		for i := 0; i < NO_OF_REDUCER_INSTANCES; i++ {
			_, err := conn.Do("SADD", "started-reducer-"+strconv.Itoa(i), statusMessage.Id)
			if err != nil {
				return fmt.Errorf("error pushing value to set in redis: %v", err)
			}
		}
	} else if statusMessage.Status == STATUS_FINISHED {
		// If the status is "finished", then we need to remove the id from the "started" set in redis
		_, err := conn.Do("SREM", "started-reducer-"+statusMessage.ReducerNum, statusMessage.Id)
		if err != nil {
			return fmt.Errorf("error removing value from set in redis: %v", err)
		}
		// Check if the "started" set is empty
		cardinality, err := conn.Do("SCARD", "started-reducer-"+statusMessage.ReducerNum)
		if err != nil {
			return fmt.Errorf("error checking if set is empty: %v", err)
		}
		if cardinality == 0 {
			// If the set is empty, then we need to send a message to start generating the output files
			// Create a client for the controller topic
			outputterTopic := client.Topic("mapreduce-outputter-" + statusMessage.ReducerNum)
			defer outputterTopic.Stop()
			SendPubSubMessage(ctx, nil, nil, statusMessage, nil)
		}
	}
	return nil
}
