package shuffle_phase

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
)

// Combine is a function that is triggered by a message being published to the Combine topic. It receives the list of
// key-value pairs from the mapper, and does a mini-reduce to group the key-value pairs by key. It requires the message
// data to be of type []WordData.
func Combine(ctx context.Context, e event.Event) error {
	// Read the data from the event i.e. message pushed from mapper
	var wordData []tools.WordData
	client, attributes, err := tools.ReadPubSubMessage(ctx, e, &wordData)
	if err != nil {
		return err
	}
	defer client.Close()
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
	combinedKeyValues := make([]tools.WordData, 0)
	for k, v := range combinedWordDataMap {
		combinedKeyValues = append(combinedKeyValues, tools.WordData{SortedWord: k, Anagrams: v})
	}
	// Send the combined key-value pairs to the Shuffler topic
	tools.SendPubSubMessage(ctx, nil, client.Topic(tools.SHUFFLER_TOPIC), combinedKeyValues, attributes)
	return nil
}
