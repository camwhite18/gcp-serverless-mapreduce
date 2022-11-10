package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const NO_OF_MAPPER_INSTANCES = 5

func init() {
	functions.HTTP("StartMapreduce", startMapreduce)
}

type Response struct {
	ResponseCode int    `json:"responseCode"`
	Message      string `json:"message"`
}

func startMapreduce(w http.ResponseWriter, r *http.Request) {
	// Read bucket name from request
	bucketName := r.URL.Query().Get("bucket")
	// Create a new storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	// Iterate over all objects in the bucket
	objects := client.Bucket(bucketName).Objects(ctx, nil)
	files := make([]string, 0)
	for {
		attributes, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Add the file name to the list of files
		files = append(files, attributes.Name)
	}
	// If there are no files in the bucket, return an error
	if len(files) == 0 {
		writeResponse(w, http.StatusBadRequest, "No files found in bucket")
		return
	}

	// Split the slice into NO_OF_MAPPER_INSTANCES slices
	splitFiles := make([][]string, NO_OF_MAPPER_INSTANCES)
	for i, file := range files {
		splitFiles[i%NO_OF_MAPPER_INSTANCES] = append(splitFiles[i%NO_OF_MAPPER_INSTANCES], file)
	}
	// Send the slices to the splitter instances
	ctxBackground := context.Background()
	var wg sync.WaitGroup
	for i, files := range splitFiles {
		wg.Add(1)
		go sendToSplitter(ctxBackground, &wg, bucketName, files, i)
	}
	wg.Wait()
	writeResponse(w, http.StatusOK, "MapReduce started successfully")
}

func sendToSplitter(ctx context.Context, wg *sync.WaitGroup, bucketName string, files []string, instanceNo int) {
	defer wg.Done()
	// Create a new pubsub client
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		log.Printf("Error creating client: %v", err)
	}
	defer client.Close()
	// Set the topic the client will publish to
	splitterNo := strconv.Itoa(instanceNo)
	topic := client.Topic("mapreduce-splitter-" + splitterNo)
	for _, file := range files {
		// Read the contents of the file
		data, err := readFileFromBucket(ctx, bucketName, file)
		if err != nil {
			log.Printf("Error reading file %s from bucket %s: %v", file, bucketName, err)
		}
		// Send the contents to the splitter instance
		result := topic.Publish(ctx, &pubsub.Message{
			Data:        removeBookHeaderAndFooter(data),
			Attributes:  map[string]string{"splitter": splitterNo},
			PublishTime: time.Now(),
		})
		// Get the result of the publish
		id, err := result.Get(ctx)
		if err != nil {
			log.Printf("Error publishing message to topic %s: %v", topic, err)
		}
		log.Printf("Published a message to topic mapreduce-splitter-%s; msg ID: %v", strconv.Itoa(instanceNo), id)
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

func writeResponse(w http.ResponseWriter, code int, message string) {
	responseMsg := Response{
		ResponseCode: code,
		Message:      message,
	}
	responseMsgBytes, err := json.Marshal(responseMsg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(code)
	w.Write(responseMsgBytes)
}
