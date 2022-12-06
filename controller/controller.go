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
// splitter for each file partition and adds the partitions uuid to a set in redis. It is also triggered by the shuffler
// once a partition has been added to the redis instances. A message is then sent to the reducer to start reducing the
// data in each redis instance once the Redis set cardinality is 0.
func Controller(ctx context.Context, e event.Event) error {
	r.InitSingleRedisClient()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from splitter or reducer
	var statusMessage pubsub.ControllerMessage
	attributes, err := pubsubClient.ReadPubSubMessage(&statusMessage)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	// We need to perform different actions depending on the status of the message
	switch statusMessage.Status {
	// If the status is "started", then we add the partition uuid to the set in redis
	case pubsub.StatusStarted:
		// Use SADD to add the partition uuid to the 'started-processing' set in redis
		res := r.SingleRedisClient.SAdd(ctx, "started-processing", statusMessage.ID)
		if res.Err() != nil {
			return fmt.Errorf("error pushing value to set in redis: %v", res.Err())
		}
	// If the status is "finished", then we remove the partition uuid from the set in redis, and check if the set is empty.
	case pubsub.StatusFinished:
		res := r.SingleRedisClient.SRem(ctx, "started-processing", statusMessage.ID)
		if res.Err() != nil {
			return fmt.Errorf("error removing value from set in redis: %v", res.Err())
		}
		// Check if the set is empty, and start reducing if it is
		err = checkSetCardinality(ctx, pubsubClient, attributes)
		if err != nil {
			return fmt.Errorf("error removing uuid from redis set: %v", err)
		}
	}
	return nil
}

// checkSetCardinality checks the cardinality of the set in redis. If the set is empty, then it sends a message to the
// reducer topic for each redis instance to start a reducing job on each
func checkSetCardinality(ctx context.Context, client pubsub.Client, attributes map[string]string) error {
	// Use SCARD to get the cardinality of the 'started-processing' set in redis
	cardinality, err := r.SingleRedisClient.SCard(ctx, "started-processing").Result()
	if err != nil {
		return fmt.Errorf("error checking if set is empty: %v", err)
	}
	// If the set is empty, then we need to send messages to start generating the output files
	if cardinality == int64(0) {
		// Send a message to start a reducer job on each redis instance
		var wg sync.WaitGroup
		for i := 0; i < r.NoOfReducerJobs; i++ {
			wg.Add(1)
			// Send the messages to the reducer topic concurrently to improve performance
			go func(i int) {
				defer wg.Done()
				// Create a copy of the attributes map so that we can add the redis instance number to the message
				reducerAttributes := make(map[string]string)
				for k, v := range attributes {
					reducerAttributes[k] = v
				}
				reducerAttributes["redisNum"] = strconv.Itoa(i)
				// Create a message to send to the reducer
				client.SendPubSubMessage(pubsub.ReducerTopic, nil, reducerAttributes)
			}(i)
		}
		// Wait for all the messages to be sent before returning
		wg.Wait()
	}
	return nil
}
