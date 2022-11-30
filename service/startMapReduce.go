package service

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"google.golang.org/api/iterator"
	"net/http"
	"sync"
	"time"
)

// Response is the response object sent to the client
type Response struct {
	ResponseCode int    `json:"responseCode"`
	Message      string `json:"message"`
}

// StartMapReduce is a function triggered by an HTTP request which starts the MapReduce process. It reads all the file
// names in the input bucket and pushes them to the splitter topic. The function requires two query parameters:
// input-bucket: the name of the bucket containing the input files
// output-bucket: the name of the bucket where the output files will be stored
func StartMapReduce(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// Create a new pubsub client with a blank event
	pubsubClient, err := pubsub.New(ctx, event.New())
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer pubsubClient.Close()

	// Get the query parameters
	inputBucketName := r.URL.Query().Get("input-bucket")
	if inputBucketName == "" {
		writeResponse(w, http.StatusBadRequest, "No input bucket name provided")
		return
	}
	outputBucketName := r.URL.Query().Get("output-bucket")
	if outputBucketName == "" {
		writeResponse(w, http.StatusBadRequest, "No output bucket name provided")
		return
	}
	// Read the file names in the input bucket
	files, err := readFileNamesInBucket(inputBucketName)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	// If there are no files in the bucket, write bad request response and return
	if len(files) == 0 {
		writeResponse(w, http.StatusBadRequest, "No files found in bucket")
		return
	}
	// Push each file name to the splitter topic
	// Use a wait group so we can wait for all the messages to be sent before returning
	var wg sync.WaitGroup
	for _, file := range files {
		splitterData := pubsub.SplitterData{
			BucketName: inputBucketName,
			FileName:   file,
		}
		wg.Add(1)
		// Use a goroutine to send the messages concurrently -> this is faster than sending them sequentially
		go func() {
			defer wg.Done()
			pubsubClient.SendPubSubMessage(pubsub.SPLITTER_TOPIC, splitterData, map[string]string{"outputBucket": outputBucketName})
		}()
	}
	wg.Wait()
	writeResponse(w, http.StatusOK, "MapReduce started successfully - results will be stored in: "+outputBucketName)
}

// readFileNamesInBucket reads the file names in the input bucket and returns them as a slice of strings
func readFileNamesInBucket(bucketName string) ([]string, error) {
	// Create a new storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	// Create a 10-second timeout context so we don't wait forever if there is an error
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	// Iterate over all objects in the bucket and add each file name to the files slice
	objects := client.Bucket(bucketName).Objects(ctx, nil)
	files := make([]string, 0)
	for {
		attributes, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		// Add the file name to the list of files
		files = append(files, attributes.Name)
	}
	return files, nil
}

// writeResponse writes the response to the client
func writeResponse(w http.ResponseWriter, code int, message string) {
	// Create a response object
	responseMsg := Response{
		ResponseCode: code,
		Message:      message,
	}
	// Convert the response object to JSON
	responseMsgBytes, err := json.Marshal(responseMsg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	// Write the response
	w.WriteHeader(code)
	_, _ = w.Write(responseMsgBytes)
}
