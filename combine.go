package serverless_mapreduce

import (
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

func init() {
	functions.CloudEvent("Combine", combine)
}

func combine(ctx context.Context, e event.Event) error {
	var wordData []WordData
	client, _, err := ReadPubSubMessage(ctx, e, &wordData)
	if err != nil {
		return err
	}
	defer client.Close()

	// Do a mini reduce on the data from the mapper.
	// Use map[string]struct{} to act as a set to remove duplicate words.
	combinedWordDataMap := make(map[string]map[string]struct{})
	for _, pair := range wordData {
		if combinedWordDataMap[pair.SortedWord] == nil {
			combinedWordDataMap[pair.SortedWord] = pair.Anagrams
		} else {
			for k, v := range pair.Anagrams {
				combinedWordDataMap[pair.SortedWord][k] = v
			}
		}
	}
	// Convert the map to a slice
	combinedText := make([]WordData, 0)
	for k, v := range combinedWordDataMap {
		combinedText = append(combinedText, WordData{SortedWord: k, Anagrams: v})
	}
	SendPubSubMessage(ctx, nil, client.Topic("mapreduce-shuffler"), combinedText, nil)
	return nil
}
