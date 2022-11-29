package shuffle_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
)

// Shuffler is a function that is triggered by a message being published to the Shuffler topic. It receives a list of
// WordData objects and shuffles them into a map of reducer number to a list of WordData objects. It then pushes each
// list of WordData objects to the appropriate reducer topic.
func Shuffler(ctx context.Context, e event.Event) error {
	// Read the data from the event i.e. message pushed from Combine
	var wordData []tools.WordData
	client, attributes, err := tools.ReadPubSubMessage(ctx, e, &wordData)
	if err != nil {
		return err
	}
	defer client.Close()
	// Shuffle the words into a map of reducer number a list of WordData objects
	shuffledText := shuffle(wordData)
	// Create topic client for each reducer topic
	var topics []*pubsub.Topic
	for i := 0; i < tools.NO_OF_REDUCER_INSTANCES; i++ {
		topics = append(topics, client.Topic(tools.REDUCER_TOPIC+"-"+strconv.Itoa(i)))
	}
	// Stop the topics when done
	defer func() {
		for _, topic := range topics {
			topic.Stop()
		}
	}()
	// Send each list of WordData objects to the appropriate reducer topic concurrently
	var wg sync.WaitGroup
	for reducerNum, wordData := range shuffledText {
		// Create a copy of the attributes to prevent the reducerNum from being overwritten by other goroutines
		reducerAttributes := make(map[string]string)
		for k, v := range attributes {
			reducerAttributes[k] = v
		}
		reducerAttributes["reducerNum"] = strconv.Itoa(reducerNum)
		wg.Add(1)
		log.Printf("reducerAttributes: %v", reducerAttributes)
		// Push the list of WordData objects to the appropriate reducer topic
		go tools.SendPubSubMessage(ctx, &wg, topics[reducerNum], wordData, reducerAttributes)
	}
	// Wait for all the messages to be sent
	wg.Wait()
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
