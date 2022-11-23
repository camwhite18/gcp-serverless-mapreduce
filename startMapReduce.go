package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/api/iterator"
	"log"
	"net/http"
	"sync"
	"time"
)

func init() {
	functions.HTTP("StartMapreduce", startMapreduce)
}

type Response struct {
	ResponseCode int    `json:"responseCode"`
	Message      string `json:"message"`
}

func startMapreduce(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// Get the query parameters
	bucketName := r.URL.Query().Get("bucket")
	files, err := readFileNamesInBucket(bucketName)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	// If there are no files in the bucket, write bad request response and return
	if len(files) == 0 {
		writeResponse(w, http.StatusBadRequest, "No files found in bucket")
		return
	}
	// Create a new pubsub client
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		log.Printf("Error creating client: %v", err)
		return
	}
	defer client.Close()
	// Set the topic the client will publish to
	topic := client.Topic("mapreduce-splitter")
	// Send the slices to the splitter instances
	var wg sync.WaitGroup
	for _, file := range files {
		splitterData := SplitterData{
			BucketName: bucketName,
			FileName:   file,
		}
		wg.Add(1)
		go SendPubSubMessage(ctx, &wg, topic, splitterData, nil)
	}
	wg.Wait()
	writeResponse(w, http.StatusOK, "MapReduce started successfully")
}

func readFileNamesInBucket(bucketName string) ([]string, error) {
	// Create a new storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		// Add the file name to the list of files
		files = append(files, attributes.Name)
	}
	return files, nil
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
