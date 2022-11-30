package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Reducer is a function that is triggered by a message being published to the Reducer topic. It receives the list of
// key-value pairs from the shuffler, and inserts each key value pair into the Reducer's Redis instance. It requires the
// message data to be of type []WordData.
func Reducer(ctx context.Context, e event.Event) error {
	start := time.Now()
	_, attributes, err := tools.ReadPubSubMessage(ctx, e, nil)
	if err != nil {
		return err
	}
	reducerNum := attributes["reducerNum"]
	log.Printf("Starting reducer %s", reducerNum)
	reducerNumInt, err := strconv.Atoi(reducerNum)
	if err != nil {
		return err
	}
	outputBucket := attributes["outputBucket"]
	fileName := fmt.Sprintf("anagrams-part-%s.txt", reducerNum)

	//Initialize the redis pool if it hasn't been initialized yet
	if tools.ReducerRedisPool == nil {
		var err error
		tools.ReducerRedisPool, err = tools.InitReducerRedisPool(os.Getenv("REDIS_HOSTS"))
		if err != nil {
			return fmt.Errorf("error initializing redis pool: %v", err)
		}
	}
	// Create a new storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error creating storage client: %v", err)
	}
	defer client.Close()
	// Create a writer to write to the file in the output bucket
	writer := client.Bucket(outputBucket).Object(fileName).NewWriter(ctx)
	defer writer.Close()

	// Create a channel to read the keys from redis
	keysChan := make(chan string)
	// Read the keys from redis
	readKeysFromRedis(ctx, keysChan, reducerNumInt)
	// Get the values for each key from the redis instance
	var wg sync.WaitGroup
	readAnagramsFromRedis(ctx, &wg, keysChan, writer, reducerNumInt)
	wg.Wait()
	res := tools.ReducerRedisPool[reducerNumInt].FlushAll(ctx)
	if res.Err() != nil {
		log.Printf("error flushing redis: %v", err)
	}
	log.Printf("reducer %s took %v", reducerNum, time.Since(start))
	return nil
}

func readKeysFromRedis(ctx context.Context, keysChan chan string, reducerNum int) {
	go func() {
		defer close(keysChan)
		iter := tools.ReducerRedisPool[reducerNum].Scan(ctx, 0, "", 100).Iterator()
		for iter.Next(ctx) {
			keysChan <- iter.Val()
		}
		if err := iter.Err(); err != nil {
			log.Printf("error scanning redis: %v", err)
		}
	}()
}

func readAnagramsFromRedis(ctx context.Context, wg *sync.WaitGroup, keysChan chan string, writer *storage.Writer, reducerNum int) {
	for key := range keysChan {
		res := tools.ReducerRedisPool[reducerNum].LRange(ctx, key, 0, -1)
		if res.Err() != nil {
			log.Printf("error getting value from redis: %v", res.Err())
		}
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			reducedAnagrams := reduceAnagramsAndSort(res.Val())
			if len(reducedAnagrams) > 1 {
				_, _ = writer.Write([]byte(fmt.Sprintf("%s: %s\n", key, strings.Join(reducedAnagrams, " "))))
			}
		}(key)
	}
}

func reduceAnagramsAndSort(values []string) []string {
	var reducedAnagrams []string
	var anagramMap = make(map[string]struct{})
	for _, value := range values {
		anagramMap[value] = struct{}{}
	}
	for key := range anagramMap {
		reducedAnagrams = append(reducedAnagrams, key)
	}
	sort.Strings(reducedAnagrams)
	return reducedAnagrams
}
