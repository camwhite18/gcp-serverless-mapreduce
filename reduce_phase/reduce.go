package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
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
	conn := readKeysFromRedis(keysChan, reducerNumInt)
	defer conn.Close()
	// Get the values for each key from the redis instance
	var wg sync.WaitGroup
	readAnagramsFromRedis(&wg, keysChan, writer, reducerNumInt)
	wg.Wait()
	_, err = conn.Do("FLUSHALL")
	if err != nil {
		log.Printf("error flushing redis: %v", err)
	}
	log.Printf("reducer %s took %v", reducerNum, time.Since(start))
	return nil
}

func readKeysFromRedis(keysChan chan string, reducerNum int) redis.Conn {
	// Get a connection from the redis pool for scanning the keys
	connScan := tools.ReducerRedisPool[reducerNum].Get()
	iterator := 0
	go func() {
		defer close(keysChan)
		for {
			if arr, err := redis.Values(connScan.Do("SCAN", iterator)); err != nil {
				log.Printf("error scanning redis: %v", err)
			} else {
				iterator, _ = redis.Int(arr[0], nil)
				keys, _ := redis.Strings(arr[1], nil)
				for _, key := range keys {
					keysChan <- key
				}
			}

			if iterator == 0 {
				break
			}
		}
	}()
	return connScan
}

func readAnagramsFromRedis(wg *sync.WaitGroup, keysChan chan string, writer *storage.Writer, reducerNum int) {
	defer wg.Done()
	// Get a connection from the redis pool for getting the values
	connGet := tools.ReducerRedisPool[reducerNum].Get()
	defer connGet.Close()
	for key := range keysChan {
		value, err := redis.Strings(connGet.Do("LRANGE", key, 0, -1))
		if err != nil {
			log.Printf("error getting value from redis: %v", err)
		}
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			reducedAnagrams := reduceAnagramsAndSort(value)
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
