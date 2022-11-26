package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"log"
	"strings"
	"time"
)

func init() {
	functions.CloudEvent("Outputter", outputResult)
}

func outputResult(ctx context.Context, e event.Event) error {
	start := time.Now()
	_, attributes, err := sm.ReadPubSubMessage(ctx, e, nil)
	if err != nil {
		return err
	}
	// Get the reducer number and output bucket from the attributes
	reducerNum := attributes["reducerNum"]
	outputBucket := attributes["outputBucket"]
	log.Printf("reducer %s is outputting to bucket %s", reducerNum, outputBucket)

	// Initialize the redis pool if it hasn't been initialized yet
	if sm.RedisPool == nil {
		var err error
		sm.RedisPool, err = sm.InitRedisPool()
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}
	// Connect to the redis instance for that reducer and get all the anagrams
	conn := sm.RedisPool.Get()
	defer conn.Close()
	// Get all the keys in the redis instance
	keys, err := redis.Strings(conn.Do("KEYS", "*"))
	if err != nil {
		return fmt.Errorf("error getting keys from redis: %v", err)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error creating storage client: %v", err)
	}
	defer client.Close()
	writer := client.Bucket(outputBucket).Object("result-part-" + reducerNum + ".txt").NewWriter(ctx)
	defer writer.Close()
	// Write the anagrams to a file in the output bucket
	for _, key := range keys {
		values, err := redis.Strings(conn.Do("SORT", key, "ALPHA"))
		if err != nil {
			return fmt.Errorf("error getting values from redis: %v", err)
		}
		// Add the key values to a map
		if len(values) > 1 {
			_, err := writer.Write([]byte(key + ": " + strings.Join(values, " ") + "\n"))
			if err != nil {
				return fmt.Errorf("error writing to file: %v", err)
			}
		}
	}
	// Delete the data from the redis instance
	_, err = conn.Do("FLUSHALL")
	if err != nil {
		return fmt.Errorf("error flushing redis: %v", err)
	}
	log.Printf("outputter took %v", time.Since(start))
	return nil
}
