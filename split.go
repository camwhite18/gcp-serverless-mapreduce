package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"io"
	"log"
	"math"
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
	topic := client.Topic("mapreduce-mapper-" + msg.Message.Attributes["mapper"])
	topic.PublishSettings.ByteThreshold = MAX_MESSAGE_SIZE_BYTES
	topic.PublishSettings.CountThreshold = MAX_MESSAGE_COUNT
	topic.PublishSettings.DelayThreshold = MAX_MESSAGE_DELAY
	defer topic.Stop()
	var wg sync.WaitGroup
	for _, file := range splitterData.FileNames {
		//wg.Add(1)
		sendToMapper(ctx, &wg, msg.Message.Attributes, topic, splitterData.BucketName, file, msg.Message.Data)
	}
	//wg.Wait()
	return nil
}

func sendToMapper(ctx context.Context, wg *sync.WaitGroup, attributes map[string]string, topic *pubsub.Topic,
	bucketName string, file string, data []byte) {
	//defer wg.Done()
	data, err := readFileFromBucket(ctx, bucketName, file)
	if err != nil {
		log.Printf("error reading file from bucket: %v", err)
		return
	}
	data = removeBookHeaderAndFooter(data)
	// Split the file into a list of words
	splitText := strings.Fields(string(data))
	numOfPartitions := 1
	log.Printf("splitText length: %v", len(splitText))
	// Convert to byte array and calculate the number of partitions
	byteArray := make([]byte, 0)
	for _, s := range splitText {
		byteArray = append(byteArray, []byte(s)...)
	}
	if size := binary.Size(byteArray); size > MAX_MESSAGE_SIZE_BYTES {
		numOfPartitions = int(math.Ceil(float64(size) / MAX_MESSAGE_SIZE_BYTES))
		log.Printf("Size of data is %d bytes, so splitting into %d partitions", size, numOfPartitions)
	}
	// Split the list of words into partitions of less than MAX_MESSAGE_SIZE_BYTES bytes
	partitionSize := int(math.Ceil(float64(len(splitText)) / float64(numOfPartitions)))
	for i := 0; i < len(splitText); i += partitionSize {
		end := i + partitionSize
		if end > len(splitText) {
			end = len(splitText)
		}
		partition := splitText[i:end]
		// Send partition to mapper
		mapperData := MapperData{
			Text: partition,
		}
		data, err = json.Marshal(mapperData)
		if err != nil {
			log.Printf("error marshalling mapper data: %v", err)
			return
		}
		result := topic.Publish(ctx, &pubsub.Message{
			Data:       data,
			Attributes: attributes,
		})
		_, err = result.Get(ctx)
		if err != nil {
			log.Printf("error publishing message to topic: %v", err)
		}
	}
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
