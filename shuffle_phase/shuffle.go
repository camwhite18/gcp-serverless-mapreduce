package shuffle_phase

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"time"
)

// Shuffler is a function that is triggered by a message being published to the Shuffler topic. It receives a list of
// WordData objects and shuffles them into a map of reducer number to a list of WordData objects. It then pushes each
// list of WordData objects to the appropriate reducer topic.
func Shuffler(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Read the data from the event i.e. message pushed from Combine
	var wordData []tools.WordData
	client, attributes, err := tools.ReadPubSubMessage(ctx, e, &wordData)
	if err != nil {
		return err
	}
	defer client.Close()
	// Shuffle the words into a map of reducer number to a list of WordData objects
	shuffledText := shuffle(wordData)
	// Add each list of WordData objects to the appropriate reducer number
	err = addToRedis(ctx, shuffledText)
	if err != nil {
		return err
	}
	// Send a message to the controller topic to let it know that the shuffling is complete for the partition
	topic := client.Topic(tools.CONTROLLER_TOPIC)
	defer topic.Stop()
	statusMessage := tools.StatusMessage{
		Id:     attributes["partitionId"],
		Status: tools.STATUS_FINISHED,
	}
	tools.SendPubSubMessage(ctx, nil, topic, statusMessage, attributes)
	log.Printf("Shuffling took %v", time.Since(start))
	return nil
}

// shuffle takes a list of WordData objects and shuffles them into a map of reducer number to a list of WordData objects
func shuffle(wordData []tools.WordData) map[int][]tools.WordData {
	// Create a map of reducer number to a list of WordData objects
	shuffledText := make(map[int][]tools.WordData)
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Loop through each WordData object and add it to the appropriate reducer number concurrently
	for _, value := range wordData {
		wg.Add(1)
		go func(value tools.WordData) {
			defer wg.Done()
			// Get the reducer number for a given sorted word
			reducerNum := partition(value.SortedWord)
			// Lock the map to prevent concurrent writes
			mu.Lock()
			// Add the WordData object to the appropriate reducer number
			if shuffledText[reducerNum] == nil {
				shuffledText[reducerNum] = make([]tools.WordData, 0)
			}
			shuffledText[reducerNum] = append(shuffledText[reducerNum], value)
			// Unlock the map
			mu.Unlock()
		}(value)
	}
	// Wait for all the words to be added to the map
	wg.Wait()
	return shuffledText
}

func addToRedis(ctx context.Context, shuffledText map[int][]tools.WordData) error {
	// Create a pool of connections to the Redis server
	if tools.ReducerRedisPool == nil {
		var err error
		tools.ReducerRedisPool, err = tools.InitReducerRedisPool(os.Getenv("REDIS_HOSTS"))
		if err != nil {
			return err
		}
	}
	// Add each list of WordData objects to the appropriate reducer number
	var wg sync.WaitGroup
	for reducerNum := range shuffledText {
		wg.Add(1)
		go func(reducerNum int) {
			defer wg.Done()
			// Get a connection from the pool
			for _, value := range shuffledText[reducerNum] {
				// Add the WordData object to the appropriate reducer number
				var anagrams []interface{}
				for word := range value.Anagrams {
					anagrams = append(anagrams, word)
				}
				// Add the word to the appropriate reducer number
				res := tools.ReducerRedisPool[reducerNum].LPush(ctx, value.SortedWord, anagrams...)
				if res.Err() != nil {
					log.Println(res.Err())
				}
			}
		}(reducerNum)
	}
	// Wait for all the words to be added to redis
	wg.Wait()
	return nil
}

// partition takes a word and returns the reducer number it should be sent to by taking the modulus of the
// hashed word with the total number of reducers
func partition(s string) int {
	// Create a new hash
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	hashedString := h.Sum32()
	// Take the modulus of the hashed word with the total number of reducers
	return int(hashedString % uint32(tools.NO_OF_REDUCER_INSTANCES))
}
