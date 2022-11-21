package serverless_mapreduce

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"os"
)

func init() {
	functions.CloudEvent("Shuffler", shuffler)
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

func shuffler(_ context.Context, e event.Event) error {
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
	for _, wordData := range wordDataSlice {
		li, err := conn.Do("GET", wordData.SortedWord)
		if err != nil {
			return fmt.Errorf("error getting data from redis: %v", err)
		}
		var words []string
		if li == nil {
			words = []string{wordData.Word}
		} else {
			err := json.Unmarshal(li.([]byte), &words)
			if err != nil {
				return fmt.Errorf("rror unmarshalling data from redis: %v", err)
			}
			words = append(words, wordData.Word)
		}
		liBytes, err := json.Marshal(words)
		if err != nil {
			return fmt.Errorf("error marshalling data to redis: %v", err)
		}
		_, err = conn.Do("SET", wordData.SortedWord, liBytes)
		if err != nil {
			return fmt.Errorf("error setting data in redis: %v", err)
		}
	}
	return nil
}
