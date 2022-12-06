package reducephase

import (
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	r "gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
	"time"
)

// Shuffler is a function that is triggered by a message being published to the Shuffler topic. It receives a list of
// MappedWord objects and shuffles them into a map of reducer number to a list of MappedWord objects. It then writes
// each list of MappedWord objects to the appropriate redis instance. The sorting phase of MapReduce happens through how
// the data is stored in Redis - it is stored in lists meaning all the anagrams for a given word are stored together.
// It then sends a message to the controller topic to let it know that the shuffling is complete for the partition.
func Shuffler(ctx context.Context, e event.Event) error {
	start := time.Now()
	r.InitMultiRedisClient()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from Combiner
	var wordData []pubsub.MappedWord
	attributes, err := pubsubClient.ReadPubSubMessage(&wordData)
	if err != nil {
		return err
	}

	// Shuffle the words into a map of reducer number to a list of MappedWord objects
	shuffledText := shuffle(wordData)
	// Add each list of MappedWord objects to the correct redis instance
	err = addToRedis(ctx, shuffledText)
	if err != nil {
		return fmt.Errorf("error adding to redis: %v", err)
	}
	// Send a message to the controller topic to let it know that the shuffling is complete for the partition
	statusMessage := pubsub.ControllerMessage{
		ID:     attributes["partitionId"],
		Status: pubsub.StatusFinished,
	}
	pubsubClient.SendPubSubMessage(pubsub.ControllerTopic, statusMessage, attributes)
	log.Printf("Shuffling took %v", time.Since(start))
	return nil
}

// shuffle takes a list of MappedWord objects and shuffles them into a map of reducer number to a list of MappedWord
// objects
func shuffle(wordData []pubsub.MappedWord) map[int][]pubsub.MappedWord {
	shuffledText := make(map[int][]pubsub.MappedWord)
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Loop through each MappedWord object and add it to the appropriate reducer number concurrently
	for _, value := range wordData {
		wg.Add(1)
		go func(value pubsub.MappedWord) {
			defer wg.Done()
			// Get the reducer number for a given sorted word
			reducerNum := partitioner(value.SortedWord)
			// Lock the mutex to prevent concurrent writes to the map
			mu.Lock()
			defer mu.Unlock()
			// Add the MappedWord object to the appropriate reducer number
			if shuffledText[reducerNum] == nil {
				shuffledText[reducerNum] = make([]pubsub.MappedWord, 0)
			}
			shuffledText[reducerNum] = append(shuffledText[reducerNum], value)
		}(value)
	}
	// Wait for all the words to be added to the map
	wg.Wait()
	return shuffledText
}

// partitioner takes a word and returns the reducer number it should be sent to by taking the modulus of the
// hashed word with the total number of reducers
func partitioner(s string) int {
	// Create a new hash
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	hashedString := h.Sum32()
	// Take the modulus of the hashed word with the total number of reducers
	return int(hashedString % uint32(r.NoOfReducerJobs))
}

// addToRedis takes a map of reducer number to a list of MappedWord objects and adds each list of MappedWord objects
// to the appropriate redis instance
func addToRedis(ctx context.Context, shuffledText map[int][]pubsub.MappedWord) error {
	var err error
	var wg sync.WaitGroup
	// Loop through each reducer number and add the list of MappedWord objects to the appropriate redis instance
	for reducerNum := range shuffledText {
		wg.Add(1)
		go func(reducerNum int) {
			defer wg.Done()
			// Loop through each MappedWord object and add it to the redis instance
			for _, value := range shuffledText[reducerNum] {
				// Convert the map to a slice of interfaces
				var anagrams []interface{}
				for word := range value.Anagrams {
					anagrams = append(anagrams, word)
				}
				// Push the anagrams into the list for the sorted word in the redis instance, this emulates the job of
				// the sort phase of MapReduce
				res := r.MultiRedisClient[strconv.Itoa(reducerNum)].LPush(ctx, value.SortedWord, anagrams...)
				if res.Err() != nil {
					err = res.Err()
				}
			}
		}(reducerNum)
	}
	// Wait for all the words to be added to each redis instance
	wg.Wait()
	return err
}
