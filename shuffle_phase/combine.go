package shuffle_phase

import (
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce"
)

func init() {
	functions.CloudEvent("Combine", combine)
}

func combine(ctx context.Context, e event.Event) error {
	var wordData []serverless_mapreduce.WordData
	client, attributes, err := serverless_mapreduce.ReadPubSubMessage(ctx, e, &wordData)
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
	combinedText := make([]serverless_mapreduce.WordData, 0)
	for k, v := range combinedWordDataMap {
		combinedText = append(combinedText, serverless_mapreduce.WordData{SortedWord: k, Anagrams: v})
	}
	serverless_mapreduce.SendPubSubMessage(ctx, nil, client.Topic("mapreduce-shuffler"), combinedText, attributes)
	return nil
}
