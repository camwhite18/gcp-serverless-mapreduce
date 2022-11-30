package controller

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	r "gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"strconv"
	"sync"
)

// Controller is a function that is triggered by a message being published to the controller topic. It is triggered by the
// splitter for each file partition and adds the partitions uuid to each "started-reducer-{0,..,N-1}" set in redis. It is also
// triggered by the reducer for each slice of MappedWord it receives. It removes the uuid for that partition from that
// reducer's set and checks if the set is empty. If it is empty, then it sends a message to the outputter to start.
func Controller(ctx context.Context, e event.Event) error {
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()
	//Initialize the controller redis pool if it hasn't been initialized yet
	r.InitRedisClient()

	// Read the data from the event i.e. message pushed from splitter or reducer
	var statusMessage pubsub.ControllerMessage
	attributes, err := pubsubClient.ReadPubSubMessage(&statusMessage)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	switch statusMessage.Status {
	case pubsub.STATUS_STARTED:
		// If the status is "started", then we need to add the partition uuid to each "started-reducer-{0,..,N-1}" set in redis
		res := r.RedisClient.SAdd(ctx, "started-processing", statusMessage.Id)
		if res.Err() != nil {
			return fmt.Errorf("error pushing value to set in redis: %v", res.Err())
		}
	case pubsub.STATUS_FINISHED:
		// If the status is "finished", then we need to remove the partition uuid from "started-reducer-X" set in redis
		// and check if the set is empty. If it is empty, then we need to send a message to start generating the output files
		err = removeUUIDFromRedisSet(ctx, pubsubClient, attributes, statusMessage.Id)
		if err != nil {
			return fmt.Errorf("error removing uuid from redis set: %v", err)
		}
	}
	return nil
}

// removeUUIDFromRedisSet removes the id from the "started-reducer-X" set in redis and checks if the set is empty. If it is
// empty, then it sends a message to the outputter to start.
func removeUUIDFromRedisSet(ctx context.Context, client pubsub.Client, attributes map[string]string,
	id string) error {
	// If the status is "finished", then we remove the partition uuid from the "started-reducer-X" set in redis
	res := r.RedisClient.SRem(ctx, "started-processing", id)
	if res.Err() != nil {
		return fmt.Errorf("error removing value from set in redis: %v", res.Err())
	}
	// Check if the "started-reducer-X" set is empty
	cardinality, err := r.RedisClient.SCard(ctx, "started-processing").Result()
	if err != nil {
		return fmt.Errorf("error checking if set is empty: %v", err)
	}
	// If the set is empty, then we need to send a message to start generating the output files
	if cardinality == int64(0) {
		// Send a message to start a reducer on each redis instance
		var wg sync.WaitGroup
		for i := 0; i < r.NO_OF_REDUCER_INSTANCES; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				reducerAttributes := make(map[string]string)
				for k, v := range attributes {
					reducerAttributes[k] = v
				}
				reducerAttributes["reducerNum"] = strconv.Itoa(i)
				// Create a message to send to the reducer
				client.SendPubSubMessage(pubsub.REDUCER_TOPIC, nil, reducerAttributes)
			}(i)
		}
		wg.Wait()
	}
	return nil
}
