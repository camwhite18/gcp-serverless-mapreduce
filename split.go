package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"io"
	"log"
	"regexp"
	"strings"
	"sync"
)

func init() {
	functions.CloudEvent("Splitter", splitter)
}

func splitter(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("error getting data from event: %v", err)
	}
	// Unmarshal message data
	splitterData := SplitterData{}
	if err := json.Unmarshal(msg.Message.Data, &splitterData); err != nil {
		return fmt.Errorf("error unmarshalling message: %v", err)
	}
	// Loop through all files in the bucket, then send each file to the mapper
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		return fmt.Errorf("error creating pubsub client: %v", err)
	}
	defer client.Close()
	var wg sync.WaitGroup
	for _, file := range splitterData.FileNames {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			data, err := readFileFromBucket(ctx, splitterData.BucketName, file)
			if err != nil {
				log.Printf("error reading file from bucket: %v", err)
				return
			}
			data = removeBookHeaderAndFooter(data)
			// Split the file into a list of words
			splitText := strings.Fields(string(data))
			// Send file to mapper
			mapperData := MapperData{
				Text: splitText,
			}
			data, err = json.Marshal(mapperData)
			if err != nil {
				log.Printf("error marshalling mapper data: %v", err)
				return
			}
			topic := client.Topic("mapreduce-mapper-" + msg.Message.Attributes["mapper"])
			result := topic.Publish(ctx, &pubsub.Message{
				Data:       data,
				Attributes: msg.Message.Attributes,
			})
			_, err = result.Get(ctx)
			if err != nil {
				log.Printf("error publishing message to topic: %v", err)
			}
		}(file)
	}
	wg.Wait()
	return nil
}

func readFileFromBucket(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func removeBookHeaderAndFooter(data []byte) []byte {
	// remove book header
	re := regexp.MustCompile(`\*\*\*.*START OF TH(E|IS) PROJECT GUTENBERG EBOOK.*\*\*\*`)
	// find the index of the occurrence of the header
	index := re.FindStringIndex(string(data))
	// remove the header
	if index != nil {
		data = data[index[1]+1:]
	}
	// remove book footer
	re = regexp.MustCompile(`\*\*\*.*END OF TH(E|IS) PROJECT GUTENBERG EBOOK.*\*\*\*`)
	// find the index of the occurrence of the footer
	index = re.FindStringIndex(string(data))
	// remove the footer
	if index != nil {
		data = data[:index[0]]
	}
	return data
}
