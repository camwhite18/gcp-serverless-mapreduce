package map_phase

import (
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/storage"
	"net/http"
	"strings"
	"sync"
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
	// Get the query parameters
	inputBucketName := r.URL.Query().Get("input-bucket")
	if inputBucketName == "" {
		writeResponse(w, http.StatusBadRequest, "No input bucket name provided, please provide one using the query parameter 'input-bucket'")
		return
	}
	outputBucketName := r.URL.Query().Get("output-bucket")
	if outputBucketName == "" {
		writeResponse(w, http.StatusBadRequest, "No output bucket name provided, please provide one using the query parameter 'output-bucket'")
		return
	}
	// Create a storage client
	storageClient, err := storage.New(ctx)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer storageClient.Close()
	// Read the file names in the input bucket
	files, err := storageClient.ReadObjectNames(ctx, inputBucketName)
	if err != nil {
		if strings.Contains(err.Error(), "bucket doesn't exist") {
			writeResponse(w, http.StatusBadRequest, "Storage bucket doesn't exist or isn't accessible")
			return
		}
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	// If there are no files in the bucket, write bad request response and return
	if len(files) == 0 {
		writeResponse(w, http.StatusBadRequest, "No files found in input bucket: "+inputBucketName)
		return
	}
	// Create a new pubsub client with a blank event
	pubsubClient, err := pubsub.New(ctx, event.New())
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer pubsubClient.Close()
	// Push each file name to the splitter topic
	var wg sync.WaitGroup
	for _, file := range files {
		// Create the data that will be sent to the splitter
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
	// Use a wait group so we can wait for all the messages to be sent before sending a response
	wg.Wait()
	writeResponse(w, http.StatusOK, "MapReduce started successfully - results will be stored in: "+outputBucketName)
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
	w.Write(responseMsgBytes)
}
