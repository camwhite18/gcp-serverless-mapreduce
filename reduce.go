package serverless_mapreduce

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
)

var redisPool *redis.Pool

func init() {
	functions.CloudEvent("Reducer", reducer)
}

func reducer(ctx context.Context, e event.Event) error {
	//Initialize the redis pool if it hasn't been initialized yet
	if redisPool == nil {
		var err error
		redisPool, err = initRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	var wordDataSlice []WordData
	client, attributes, err := ReadPubSubMessage(context.Background(), e, &wordDataSlice)
	if err != nil {
		return fmt.Errorf("error reading pubsub message: %v", err)
	}
	conn := redisPool.Get()
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
	statusMessage := StatusMessage{
		Id:         attributes["partitionId"],
		Status:     STATUS_FINISHED,
		ReducerNum: attributes["reducerNum"],
	}
	SendPubSubMessage(ctx, nil, controllerTopic, statusMessage, nil)
	return nil
}

func initRedisPool() (*redis.Pool, error) {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		return nil, fmt.Errorf("REDIS_HOST not set")
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		return nil, fmt.Errorf("REDIS_PORT not set")
	}
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)

	const maxConnections = 10
	return &redis.Pool{
		MaxIdle: maxConnections,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddress)
		},
	}, nil
}
