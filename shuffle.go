package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"hash/fnv"
	"strconv"
	"sync"
)

func init() {
	functions.CloudEvent("Shuffler", shuffler)
}

func shuffler(ctx context.Context, e event.Event) error {
	var text []WordData
	client, _, err := ReadPubSubMessage(ctx, e, &text)
	if err != nil {
		return err
	}
	defer client.Close()
	// Shuffle the words into a map of reducer number to a list of words
	shuffledText := shuffle(text)
	// Create topic object for each reducer
	var topics []*pubsub.Topic
	for i := 0; i < NO_OF_REDUCER_INSTANCES; i++ {
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
		wg.Add(1)
		go SendPubSubMessage(ctx, &wg, topics[reducerNum], wordData, nil)
	}
	wg.Wait()
	return nil
}

func shuffle(keyValues []WordData) map[int][]WordData {
	shuffledText := make(map[int][]WordData)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, value := range keyValues {
		wg.Add(1)
		go func(value WordData) {
			defer wg.Done()
			reducerNum := partition(value.SortedWord)
			mu.Lock()
			if shuffledText[reducerNum] == nil {
				shuffledText[reducerNum] = make([]WordData, 0)
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
	return int(hashedString % uint32(NO_OF_REDUCER_INSTANCES))
}
