package controller

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"strconv"
)

// Controller is a function that is triggered by a message being published to the controller topic. It is triggered by the
// splitter for each file partition and adds the partitions uuid to each "started-reducer-{0,..,N-1}" set in redis. It is also
// triggered by the reducer for each slice of WordData it receives. It removes the uuid for that partition from that
// reducer's set and checks if the set is empty. If it is empty, then it sends a message to the outputter to start.
func Controller(ctx context.Context, e event.Event) error {
	//Initialize the controller redis pool if it hasn't been initialized yet
	if tools.RedisPool == nil {
		var err error
		tools.RedisPool, err = tools.InitRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	// Read the data from the event i.e. message pushed from splitter or reducer
	var statusMessage tools.StatusMessage
	client, attributes, err := tools.ReadPubSubMessage(context.Background(), e, &statusMessage)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	// Get a connection from the redis pool
	conn := tools.RedisPool.Get()
	defer conn.Close()
	if statusMessage.Status == tools.STATUS_STARTED {
		// If the status is "started", then we need to add the partition uuid to each "started-reducer-{0,..,N-1}" set in redis
		err = addUUIDToRedisSet(conn, statusMessage.Id)
		if err != nil {
			return fmt.Errorf("error adding uuid to redis set: %v", err)
		}
	} else if statusMessage.Status == tools.STATUS_FINISHED {
		// If the status is "finished", then we need to remove the partition uuid from "started-reducer-X" set in redis
		// and check if the set is empty. If it is empty, then we need to send a message to start generating the output files
		err = removeUUIDFromRedisSet(ctx, conn, client, attributes, statusMessage.Id)
		if err != nil {
			return fmt.Errorf("error removing uuid from redis set: %v", err)
		}
	}
	return nil
}

// addUUIDToRedisSet adds the id to each "started-reducer-{0,..,N-1}" set in redis
func addUUIDToRedisSet(conn redis.Conn, id string) error {
	// For each reducer, add the partition uuid to the "started-reducer-X" set in redis
	for i := 0; i < tools.NO_OF_REDUCER_INSTANCES; i++ {
		_, err := conn.Do("SADD", "started-reducer-"+strconv.Itoa(i), id)
		if err != nil {
			return fmt.Errorf("error pushing value to set in redis: %v", err)
		}
	}
	return nil
}

// removeUUIDFromRedisSet removes the id from the "started-reducer-X" set in redis and checks if the set is empty. If it is
// empty, then it sends a message to the outputter to start.
func removeUUIDFromRedisSet(ctx context.Context, conn redis.Conn, client *pubsub.Client, attributes map[string]string,
	id string) error {
	// If the status is "finished", then we remove the partition uuid from the "started-reducer-X" set in redis
	_, err := conn.Do("SREM", "started-reducer-"+attributes["reducerNum"], id)
	if err != nil {
		return fmt.Errorf("error removing value from set in redis: %v", err)
	}
	// Check if the "started-reducer-X" set is empty
	cardinality, err := conn.Do("SCARD", "started-reducer-"+attributes["reducerNum"])
	if err != nil {
		return fmt.Errorf("error checking if set is empty: %v", err)
	}
	// If the set is empty, then we need to send a message to start generating the output files
	if cardinality == int64(0) {
		// Create a client for the controller topic
		outputterTopic := client.Topic(tools.OUTPUTTER_TOPIC + "-" + attributes["reducerNum"])
		defer outputterTopic.Stop()
		tools.SendPubSubMessage(ctx, nil, outputterTopic, nil, attributes)
	}
	return nil
}
