package reduce_phase

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	r "gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"gitlab.com/cameron_w20/serverless-mapreduce/storage"
	"log"
	"sort"
	"sync"
	"time"
)

// Reducer is a function that is triggered by a message being published to the Reducer topic. It accesses a Redis
// instance and reads the key-value pairs that were written by the shuffler. It then removes any duplicate anagrams,
// sorts them alphabetically and writes the key-value pairs to a file in the output bucket.
func Reducer(ctx context.Context, e event.Event) error {
	start := time.Now()
	r.InitMultiRedisClient()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from controller
	attributes, err := pubsubClient.ReadPubSubMessage(nil)
	if err != nil {
		return err
	}
	// Get the redis number and the output bucket from the attributes
	redisNum := attributes["redisNum"]
	outputBucket := attributes["outputBucket"]
	fileName := fmt.Sprintf("anagrams-part-%s.txt", redisNum)

	// Read, reduce and write to a file the key-value pairs from redis
	err = reduceAnagramsFromRedis(ctx, outputBucket, fileName, redisNum)
	if err != nil {
		return err
	}
	// Remove all the data from the redis instance after it has been processed
	res := r.MultiRedisClient[redisNum].FlushAll(ctx)
	if res.Err() != nil {
		log.Printf("error flushing redis: %v", err)
	}
	log.Printf("reducer %s took %v", redisNum, time.Since(start))
	return nil
}

// reduceAnagramsFromRedis reads the key-value pairs from redis, reduces them and writes them to a file in the output
// bucket. It uses a mutex to prevent race conditions when writing to the file.
func reduceAnagramsFromRedis(ctx context.Context, outputBucket, fileName, redisNum string) error {
	// Create a new storage client to write the output file
	storageClient, err := storage.NewWithWriter(ctx, outputBucket, fileName)
	if err != nil {
		return err
	}
	defer storageClient.Close()

	// Get all the keys from the redis instance
	keys := r.MultiRedisClient[redisNum].Keys(ctx, "*").Val()

	var wg sync.WaitGroup
	var mu sync.Mutex
	// Loop through each key and reduce the list of anagrams for that key and write it to the file in the output bucket
	// concurrently
	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			// Use LRange to get all the values in the list for the key
			res := r.MultiRedisClient[redisNum].LRange(ctx, key, 0, -1)
			if res.Err() != nil {
				err = fmt.Errorf("error getting value from redis: %v", res.Err())
				return
			}
			// Remove any duplicate anagrams in the slice
			reducedAnagrams := reduceAnagrams(res.Val())
			// Only write to the file if the key has more than one anagram
			if len(reducedAnagrams) > 1 {
				// Sort the anagrams alphabetically
				sort.Strings(reducedAnagrams)
				// Write the key-value pairs to the output file and use a mutex to prevent race conditions
				mu.Lock()
				storageClient.WriteData(key, reducedAnagrams)
				mu.Unlock()
			}
		}(key)
	}
	// Wait until all the key, list of anagrams pairs have been processed
	wg.Wait()
	return err
}

// reduceAnagrams removes any duplicate anagrams from the slice
func reduceAnagrams(values []string) []string {
	var reducedAnagrams []string
	// Create a map to ignore duplicate anagrams
	var anagramMap = make(map[string]struct{})
	// Loop through each anagram and add it to the map
	for _, value := range values {
		anagramMap[value] = struct{}{}
	}
	// Convert the map back to a slice
	for key := range anagramMap {
		reducedAnagrams = append(reducedAnagrams, key)
	}
	return reducedAnagrams
}
