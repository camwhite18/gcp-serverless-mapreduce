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

func init() {
	functions.CloudEvent("Reducer", reducer)
}

var redisPool *redis.Pool

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

func reducer(_ context.Context, e event.Event) error {
	//Initialize the redis pool if it hasn't been initialized yet
	if redisPool == nil {
		var err error
		redisPool, err = initRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}

	var wordDataSlice []WordData
	_, _, err := ReadPubSubMessage(context.Background(), e, &wordDataSlice)
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
	return nil
}
