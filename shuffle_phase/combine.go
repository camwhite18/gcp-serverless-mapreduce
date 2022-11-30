package shuffle_phase

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"log"
	"time"
)

// Combine is a function that is triggered by a message being published to the Combine topic. It receives the list of
// key-value pairs from the mapper, and does a mini-reduce to group the key-value pairs by key. It requires the message
// data to be of type []WordData.
func Combine(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from mapper
	var wordData []pubsub.MappedWord
	attributes, err := pubsubClient.ReadPubSubMessage(&wordData)
	if err != nil {
		return err
	}
	// Use map[string]struct{} as the value to act as a set to not consider duplicate words
	combinedWordDataMap := make(map[string]map[string]struct{})
	// Combine the key-value pairs
	for _, pair := range wordData {
		if combinedWordDataMap[pair.SortedWord] == nil {
			combinedWordDataMap[pair.SortedWord] = pair.Anagrams
		} else {
			for k, v := range pair.Anagrams {
				combinedWordDataMap[pair.SortedWord][k] = v
			}
		}
	}
	// Convert the map to a slice of WordData
	combinedKeyValues := make([]pubsub.MappedWord, 0)
	for k, v := range combinedWordDataMap {
		combinedKeyValues = append(combinedKeyValues, pubsub.MappedWord{SortedWord: k, Anagrams: v})
	}
	// Send the combined key-value pairs to the Shuffler topic
	pubsubClient.SendPubSubMessage(pubsub.SHUFFLER_TOPIC, combinedKeyValues, attributes)
	log.Printf("Combining took %v", time.Since(start))
	return nil
}
