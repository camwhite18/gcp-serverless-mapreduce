package shuffle_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
)

func init() {
	functions.CloudEvent("Shuffler", shuffler)
}

func shuffler(ctx context.Context, e event.Event) error {
	var wordData []serverless_mapreduce.WordData
	client, attributes, err := serverless_mapreduce.ReadPubSubMessage(ctx, e, &wordData)
	if err != nil {
		return err
	}
	defer client.Close()
	// Shuffle the words into a map of reducer number to a list of words
	shuffledText := shuffle(wordData)
	// Create topic object for each reducer
	var topics []*pubsub.Topic
	for i := 0; i < serverless_mapreduce.NO_OF_REDUCER_INSTANCES; i++ {
		topics = append(topics, client.Topic("mapreduce-reducer-"+strconv.Itoa(i)))
	}
	// Stop the topics when done
	defer func() {
		for _, topic := range topics {
			topic.Stop()
		}
	}()
	// Send the shuffled words to the reducers
	var wg sync.WaitGroup
	for reducerNum, wordData := range shuffledText {
		reducerAttributes := make(map[string]string)
		for k, v := range attributes {
			reducerAttributes[k] = v
		}
		reducerAttributes["reducerNum"] = strconv.Itoa(reducerNum)
		wg.Add(1)
		log.Printf("reducerAttributes: %v", reducerAttributes)
		go serverless_mapreduce.SendPubSubMessage(ctx, &wg, topics[reducerNum], wordData, reducerAttributes)
	}
	wg.Wait()
	return nil
}

func shuffle(wordData []serverless_mapreduce.WordData) map[int][]serverless_mapreduce.WordData {
	shuffledText := make(map[int][]serverless_mapreduce.WordData)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, value := range wordData {
		wg.Add(1)
		go func(value serverless_mapreduce.WordData) {
			defer wg.Done()
			reducerNum := partition(value.SortedWord)
			mu.Lock()
			if shuffledText[reducerNum] == nil {
				shuffledText[reducerNum] = make([]serverless_mapreduce.WordData, 0)
			}
			shuffledText[reducerNum] = append(shuffledText[reducerNum], value)
			mu.Unlock()
		}(value)
	}
	wg.Wait()
	return shuffledText
}

// partition takes a word and returns the reducer number it should be sent to by taking the modulus of the
// hashed word with the total number of reducers
func partition(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	hashedString := h.Sum32()
	return int(hashedString % uint32(serverless_mapreduce.NO_OF_REDUCER_INSTANCES))
}
