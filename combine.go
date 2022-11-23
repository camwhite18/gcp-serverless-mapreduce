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

	// Combine the list of key-value pairs into a list of key-list pairs
	combinedTextMap := make(map[string][]string)
	for _, pair := range wordData {
		if combinedTextMap[pair.SortedWord] == nil {
			combinedTextMap[pair.SortedWord] = make([]string, 0)
		}
		combinedTextMap[pair.SortedWord] = append(combinedTextMap[pair.SortedWord], pair.Word)
	}
	combinedText := make([]CombinedWordData, 0)
	for key, value := range combinedTextMap {
		combinedText = append(combinedText, CombinedWordData{key, value})
	}
	SendPubSubMessage(ctx, nil, client.Topic("mapreduce-shuffler"), combinedText, nil)
	return nil
}
