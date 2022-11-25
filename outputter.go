package serverless_mapreduce

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"strings"
)

func init() {
	functions.CloudEvent("Outputter", outputResult)
}

func outputResult(ctx context.Context, e event.Event) error {
	_, attributes, err := ReadPubSubMessage(ctx, e, nil)
	if err != nil {
		return err
	}
	// Get the reducer number and output bucket from the attributes
	reducerNum := attributes["reducerNum"]
	outputBucket := attributes["outputBucket"]

	// Connect to the redis instance for that reducer and get all the anagrams
	conn := redisPool.Get()
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
	writer := client.Bucket(outputBucket).Object("reducer-" + reducerNum + "-output.txt").NewWriter(ctx)
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
	return nil
}
