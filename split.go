package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/binary"
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
	splitterData := SplitterData{}
	client, attributes, err := ReadPubSubMessage(ctx, e, &splitterData)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, file := range splitterData.FileNames {
		wg.Add(1)
		readFileAndSendToMapper(ctx, &wg, attributes, client, splitterData.BucketName, file)
	}
	wg.Wait()
	return nil
}

func readFileAndSendToMapper(ctx context.Context, wg *sync.WaitGroup, attributes map[string]string, client *pubsub.Client, bucketName, objectName string) {
	defer wg.Done()
	data, err := readFileFromBucket(ctx, bucketName, objectName)
	if err != nil {
		log.Printf("error reading file from bucket: %v", err)
		return
	}
	data = removeBookHeaderAndFooter(data)
	// Split the file into a list of words
	splitText := strings.Fields(string(data))
	partitionedText := partitionFile(splitText, MAX_MESSAGE_SIZE_BYTES)
	for _, partition := range partitionedText {
		wg.Add(1)
		go SendPubSubMessage(ctx, wg, client, "mapreduce-mapper-"+attributes["mapper"], partition, attributes)
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

func partitionFile(splitText []string, messageSize int) [][]string {
	numOfPartitions := 1
	// Convert to byte array and calculate the number of partitions
	byteArray := make([]byte, 0)
	for _, s := range splitText {
		byteArray = append(byteArray, []byte(s)...)
	}
	if size := binary.Size(byteArray); size > messageSize {
		numOfPartitions = int(math.Ceil(float64(size) / float64(messageSize)))
		log.Printf("Size of data is %d bytes, so splitting into %d partitions", size, numOfPartitions)
	}
	partitionSize := int(math.Ceil(float64(len(splitText)) / float64(numOfPartitions)))
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
