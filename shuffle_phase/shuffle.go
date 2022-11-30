package shuffle_phase

import (
	"context"
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
// MapperData objects and shuffles them into a map of reducer number to a list of MapperData objects. It then pushes each
// list of MapperData objects to the appropriate reducer topic.
func Shuffler(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from Combiner
	var wordData []pubsub.MapperData
	attributes, err := pubsubClient.ReadPubSubMessage(&wordData)
	if err != nil {
		return err
	}

	// Shuffle the words into a map of reducer number to a list of MapperData objects then add each list to the appropriate
	// redis instance as this is much faster than adding each MapperData object to redis individually
	shuffledText := shuffle(wordData)
	// Add each list of MapperData objects to the appropriate reducer number
	err = addToRedis(ctx, shuffledText)
	if err != nil {
		return err
	}
	// Send a message to the controller topic to let it know that the shuffling is complete for the partition
	//topic := client.Topic(pubsub.CONTROLLER_TOPIC)
	//defer topic.Stop()
	statusMessage := pubsub.ControllerMessage{
		Id:     attributes["partitionId"],
		Status: pubsub.STATUS_FINISHED,
	}
	pubsubClient.SendPubSubMessage(pubsub.CONTROLLER_TOPIC, statusMessage, attributes)
	log.Printf("Shuffling took %v", time.Since(start))
	return nil
}

// shuffle takes a list of MapperData objects and shuffles them into a map of reducer number to a list of MapperData objects
func shuffle(wordData []pubsub.MapperData) map[int][]pubsub.MapperData {
	shuffledText := make(map[int][]pubsub.MapperData)
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Loop through each MapperData object and add it to the appropriate reducer number concurrently
	for _, value := range wordData {
		wg.Add(1)
		go func(value pubsub.MapperData) {
			defer wg.Done()
			// Get the reducer number for a given sorted word
			reducerNum := partition(value.SortedWord)
			// Lock the map to prevent concurrent writes
			mu.Lock()
			defer mu.Unlock()
			// Add the MapperData object to the appropriate reducer number
			if shuffledText[reducerNum] == nil {
				shuffledText[reducerNum] = make([]pubsub.MapperData, 0)
			}
			shuffledText[reducerNum] = append(shuffledText[reducerNum], value)
		}(value)
	}
	// Wait for all the words to be added to the map
	wg.Wait()
	return shuffledText
}

// partition takes a word and returns the reducer number it should be sent to by taking the modulus of the
// hashed word with the total number of reducers
func partition(s string) int {
	// Create a new hash
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	hashedString := h.Sum32()
	// Take the modulus of the hashed word with the total number of reducers
	return int(hashedString % uint32(r.NO_OF_REDUCER_INSTANCES))
}

func addToRedis(ctx context.Context, shuffledText map[int][]pubsub.MapperData) error {
	r.InitMultiRedisClient()
	// Add each list of MapperData objects to the appropriate reducer number
	var wg sync.WaitGroup
	for reducerNum := range shuffledText {
		wg.Add(1)
		go func(reducerNum int) {
			defer wg.Done()
			for _, value := range shuffledText[reducerNum] {
				// Add the MapperData object to the appropriate reducer number
				var anagrams []interface{}
				for word := range value.Anagrams {
					anagrams = append(anagrams, word)
				}
				// Add the word to the appropriate reducer number
				res := r.MultiRedisClient[strconv.Itoa(reducerNum)].LPush(ctx, value.SortedWord, anagrams...)
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
