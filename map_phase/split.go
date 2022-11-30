package map_phase

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/storage"
	"log"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Splitter is a function that is triggered by a message being published to the splitter topic. It reads the file from
// the bucket, splits it into partitions and sends the partitions to the Mapper. It requires the message data to be of
// type SplitterData.
func Splitter(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from startMapReduce
	splitterData := pubsub.SplitterData{}
	attributes, err := pubsubClient.ReadPubSubMessage(&splitterData)
	if err != nil {
		return err
	}

	// Split the text in the file into partitions for efficiency and to avoid pubsub message size limits
	partitionedText, err := splitFile(ctx, splitterData.BucketName, splitterData.FileName)
	if err != nil {
		return fmt.Errorf("error splitting file: %v", err)
	}
	// Send the partitions to the Mapper
	err = sendTextToMapper(pubsubClient, attributes, partitionedText)
	if err != nil {
		return fmt.Errorf("error sending text to Mapper: %v", err)
	}
	log.Printf("Splitter took %s", time.Since(start))
	return nil
}

// splitFile reads a given file from a bucket, removes the text's header and footer, splits it into partitions and
// returns the partitions as a slice of slices of strings or an error
func splitFile(ctx context.Context, bucketName, fileName string) ([][]string, error) {
	// Create a storage client
	storageClient, err := storage.New(ctx)
	if err != nil {
		return nil, err
	}
	defer storageClient.Close()
	// Read the contents of the file from the bucket
	data, err := storageClient.ReadObject(ctx, bucketName, fileName)
	if err != nil {
		return nil, fmt.Errorf("error reading file from bucket: %v", err)
	}
	// Remove the book header and footer from the data
	data = removeBookHeaderAndFooter(data)
	// Split the file into a list of words
	splitText := strings.Fields(string(data))
	// Remove non-unique words:
	uniqueSplitText := removeDuplicateWords(splitText)
	// Partition the file since this will speed up the map phase
	partitionedText := partitionFile(uniqueSplitText, pubsub.MAX_MESSAGE_SIZE_BYTES)
	return partitionedText, nil
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

func removeDuplicateWords(text []string) []string {
	// Create a map to store the unique words
	uniqueWords := make(map[string]struct{})
	// Create a slice to store the unique words
	uniqueWordsSlice := make([]string, 0)
	// Loop through the words and add the lowercase version to the map
	for _, word := range text {
		uniqueWords[strings.ToLower(word)] = struct{}{}
	}
	// Loop through the map and add the words to the slice
	for word := range uniqueWords {
		uniqueWordsSlice = append(uniqueWordsSlice, word)
	}
	return uniqueWordsSlice
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

// sendTextToMapper sends the given text to the Mapper and returns an error
func sendTextToMapper(pubsubClient pubsub.Client, attributes map[string]string,
	partitionedText [][]string) error {
	// We need to use a wait group to wait for all the messages to be published before returning
	var wg sync.WaitGroup
	for _, partition := range partitionedText {
		// To prevent the same uuid being used for multiple messages, we need to create a new map in each goroutine
		partitionAttributes := make(map[string]string)
		for k, v := range attributes {
			partitionAttributes[k] = v
		}
		// Send the message concurrently to speed up the process
		wg.Add(1)
		go func(partition []string) {
			defer wg.Done()
			// Send a message to the controller topic to let it know that a partition has been published
			sendIDToController(pubsubClient, partitionAttributes)
			// Publish the partition to the Mapper topic
			pubsubClient.SendPubSubMessage(pubsub.MAPPER_TOPIC, partition, partitionAttributes)
		}(partition)
	}
	wg.Wait()
	return nil
}

func sendIDToController(pubsubClient pubsub.Client, attributes map[string]string) {
	// Create a unique id for the partition so that we can track it
	attributes["partitionId"] = uuid.New().String()
	statusMessage := pubsub.ControllerMessage{
		Id:     attributes["partitionId"],
		Status: pubsub.STATUS_STARTED,
	}
	// Send the message to the controller
	pubsubClient.SendPubSubMessage(pubsub.CONTROLLER_TOPIC, statusMessage, nil)
}
