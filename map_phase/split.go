package map_phase

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	sm "gitlab.com/cameron_w20/serverless-mapreduce"
	"io"
	"log"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
)

func init() {
	functions.CloudEvent("Splitter", splitter)
}

// splitter is a function that is triggered by a message being published to the splitter topic. It reads the file from
// the bucket, splits it into partitions and sends the partitions to the mapper. It requires the message data to be of
// type SplitterData.
func splitter(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Read the data from the event i.e. message pushed from startMapReduce
	splitterData := sm.SplitterData{}
	client, attributes, err := sm.ReadPubSubMessage(ctx, e, &splitterData)
	if err != nil {
		return err
	}
	defer client.Close()
	// Split the text in the file into partitions for efficiency and to avoid pubsub message size limits
	partitionedText, err := splitFile(ctx, splitterData.BucketName, splitterData.FileName)
	if err != nil {
		return fmt.Errorf("error splitting file: %v", err)
	}
	// Send the partitions to the mapper
	err = sendTextToMapper(ctx, client, attributes, partitionedText)
	if err != nil {
		return fmt.Errorf("error sending text to mapper: %v", err)
	}
	log.Printf("Splitter took %s", time.Since(start))
	return nil
}

// splitFile reads a given file from a bucket, removes the text's header and footer, splits it into partitions and
// returns the partitions as a slice of slices of strings or an error
func splitFile(ctx context.Context, bucketName, fileName string) ([][]string, error) {
	// Read the contents of the file from the bucket
	data, err := readFileFromBucket(ctx, bucketName, fileName)
	if err != nil {
		return nil, fmt.Errorf("error reading file from bucket: %v", err)
	}
	// Remove the book header and footer from the data
	data = removeBookHeaderAndFooter(data)
	// Split the file into a list of words
	splitText := strings.Fields(string(data))
	// Partition the file since this will speed up the map phase
	partitionedText := partitionFile(splitText, sm.MAX_MESSAGE_SIZE_BYTES)
	return partitionedText, nil
}

// readFileFromBucket reads a given file from a bucket and returns the data as a byte array or an error
func readFileFromBucket(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	// Create a storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	// Create a reader for the file
	rc, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	// Read the contents of the file
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// removeBookHeaderAndFooter removes the header and footer from the given text and returns the text as a byte array
func removeBookHeaderAndFooter(data []byte) []byte {
	// Create a regex to match the header
	re := regexp.MustCompile(`\*\*\*.*START OF TH(E|IS) PROJECT GUTENBERG EBOOK.*\*\*\*`)
	// Find the index of the occurrence of the header
	index := re.FindStringIndex(string(data))
	// Remove the header
	if index != nil {
		data = data[index[1]+1:]
	}
	// Create a regex to match the footer
	re = regexp.MustCompile(`\*\*\*.*END OF TH(E|IS) PROJECT GUTENBERG EBOOK.*\*\*\*`)
	// Find the index of the occurrence of the footer
	index = re.FindStringIndex(string(data))
	// Remove the footer
	if index != nil {
		data = data[:index[0]]
	}
	return data
}

// partitionFile splits the given text into partitions of a given size and returns the partitions as a slice of
// slices of strings
func partitionFile(splitText []string, messageSize int) [][]string {
	numOfPartitions := 1
	// Convert the split text to a byte array
	byteArray := make([]byte, 0)
	for _, s := range splitText {
		byteArray = append(byteArray, []byte(s)...)
	}
	// Calculate the number of partitions required and the size of each partition
	if size := binary.Size(byteArray); size > messageSize {
		numOfPartitions = int(math.Ceil(float64(size) / float64(messageSize)))
		log.Printf("Size of data is %d bytes, so splitting into %d partitions", size, numOfPartitions)
	}
	partitionSize := int(math.Ceil(float64(len(splitText)) / float64(numOfPartitions)))
	// Partition the text
	partitions := make([][]string, 0)
	for i := 0; i < len(splitText); i += partitionSize {
		end := i + partitionSize
		if end > len(splitText) {
			end = len(splitText)
		}
		partition := splitText[i:end]
		partitions = append(partitions, partition)
	}
	return partitions
}

// sendTextToMapper sends the given text to the mapper and returns an error
func sendTextToMapper(ctx context.Context, client *pubsub.Client, attributes map[string]string,
	partitionedText [][]string) error {
	// Create a client for the mapper topic
	mapperTopic := client.Topic(sm.MAPPER_TOPIC)
	defer mapperTopic.Stop()
	// Create a client for the controller topic
	controllerTopic := client.Topic(sm.CONTROLLER_TOPIC)
	defer controllerTopic.Stop()
	// We need to use a wait group to wait for all the messages to be published before returning
	var wg sync.WaitGroup
	for _, partition := range partitionedText {
		// To prevent the same uuid being used for multiple messages, we need to create a new map in each goroutine
		partitionAttributes := make(map[string]string)
		for k, v := range attributes {
			partitionAttributes[k] = v
		}
		// Create a unique id for the partition so that we can track it
		partitionAttributes["partitionId"] = uuid.New().String()
		wg.Add(2)
		// Publish the partition to the mapper topic
		go sm.SendPubSubMessage(ctx, &wg, mapperTopic, partition, partitionAttributes)
		// Send a message to the controller topic to let it know that a partition has been published
		go func() {
			statusMessage := sm.StatusMessage{
				Id:     partitionAttributes["partitionId"],
				Status: sm.STATUS_STARTED,
			}
			log.Printf("Sending status message: %v", statusMessage)
			sm.SendPubSubMessage(ctx, &wg, controllerTopic, statusMessage, nil)
		}()
	}
	wg.Wait()
	return nil
}
