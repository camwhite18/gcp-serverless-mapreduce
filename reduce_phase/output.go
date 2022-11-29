package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"log"
	"strings"
	"time"
)

// OutputAnagrams is a function that is triggered by a message being published to the outputter topic. It is triggered
// by the controller when all the data has been added to the Redis instance for a given Reducer. It retrieves all the
// anagrams from the Redis instance and writes them to a file in the output bucket.
func OutputAnagrams(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Read the data from the event i.e. message pushed from the controller
	_, attributes, err := tools.ReadPubSubMessage(ctx, e, nil)
	if err != nil {
		return err
	}
	// Get the Reducer number and output bucket from the attributes
	reducerNum := attributes["reducerNum"]
	outputBucket := attributes["outputBucket"]
	fileName := fmt.Sprintf("anagrams-part-%s.txt", reducerNum)
	// Initialize the redis pool if it hasn't been initialized yet
	if tools.RedisPool == nil {
		var err error
		tools.RedisPool, err = tools.InitRedisPool("REDIS_HOST")
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}
	// Get a connection from the redis pool
	conn := tools.RedisPool.Get()
	defer conn.Close()
	// Get all the keys in the redis instance
	keys, err := redis.Strings(conn.Do("KEYS", "*"))
	if err != nil {
		return fmt.Errorf("error getting keys from redis: %v", err)
	}
	// Write the anagrams to a file in the output bucket
	err = writeAnagramsToFile(ctx, conn, outputBucket, fileName, keys)
	if err != nil {
		return fmt.Errorf("error writing anagrams to file: %v", err)
	}
	// Delete the all the data from the redis instance
	_, err = conn.Do("FLUSHALL")
	if err != nil {
		return fmt.Errorf("error flushing redis: %v", err)
	}
	log.Printf("outputter took %v", time.Since(start))
	return nil
}

// writeAnagramsToFile writes the anagrams to a file in the output bucket. It accepts a context, a redis connection,
// the name of the output bucket, the name of the file to write to, and a slice of keys.
func writeAnagramsToFile(ctx context.Context, conn redis.Conn, outputBucket, fileName string, keys []string) error {
	// Create a new storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error creating storage client: %v", err)
	}
	defer client.Close()
	// Create a writer to write to the file in the output bucket
	writer := client.Bucket(outputBucket).Object(fileName).NewWriter(ctx)
	defer writer.Close()
	// Loop through the slice of keys and retrieve the set for each key from the redis instance
	for _, key := range keys {
		// Use the sort alpha modifier to sort the set alphabetically before retrieving it
		values, err := redis.Strings(conn.Do("SORT", key, "ALPHA"))
		if err != nil {
			return fmt.Errorf("error getting values from redis: %v", err)
		}
		// Only add the anagrams to the file if there is more than one word in the set
		if len(values) > 1 {
			// Write the anagrams to the file
			_, err := writer.Write([]byte(fmt.Sprintf("%s: %s\n", key, strings.Join(values, " "))))
			if err != nil {
				return fmt.Errorf("error writing to file: %v", err)
			}
		}
	}
	return nil
}
