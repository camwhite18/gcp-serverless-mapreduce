package reducephase

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
)

// Reducer is a function that is triggered by a message being published to the Reducer topic. It receives a message from
// the controller with the number of the redis instance to read from and the name of the output bucket in the message
// attributes. It then accesses the Redis instance and reads the sorted key-value pairs that were written by the
// shuffler. At this point, any duplicate anagrams are removed and the remaining anagrams are sorted alphabetically, and
// each key-value pair is written to a file in the output bucket if there is more than one anagram in the set.
func Reducer(ctx context.Context, e event.Event) error {
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
	// Get the redis number and the output bucket from the message attributes
	redisNum := attributes["redisNum"]
	outputBucket := attributes["outputBucket"]
	fileName := fmt.Sprintf("anagrams-part-%s.txt", redisNum)

	defer func() {
		// Remove all the data from the redis instance after returning
		res := r.MultiRedisClient[redisNum].FlushAll(ctx)
		if res.Err() != nil {
			log.Printf("error flushing redis: %v", err)
		}
	}()

	// Read, reduce and write the key-value pairs from redis to a file in the output bucket
	err = reduceAnagramsFromRedis(ctx, outputBucket, fileName, redisNum)
	if err != nil {
		return err
	}
	return nil
}

// reduceAnagramsFromRedis reads the key-value pairs from redis, removes duplicate anagrams and sorts them concurrently,
// and then writes them to a file in the output bucket if there is more than one anagram in the set. It uses a mutex to
// prevent race conditions when writing to the file.
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

// reduceAnagrams removes any duplicate anagrams from the slice by converting it to a map and then back to a slice
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
