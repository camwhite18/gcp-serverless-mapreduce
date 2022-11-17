package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const DEFAULT_NO_OF_MAPPER_INSTANCES = 5
const DEFAULT_NO_OF_REDUCER_INSTANCES = 5

func init() {
	functions.HTTP("StartMapreduce", startMapreduce)
}

type Response struct {
	ResponseCode int    `json:"responseCode"`
	Message      string `json:"message"`
}

func startMapreduce(w http.ResponseWriter, r *http.Request) {
	// Create mapreduce instance uuid
	instanceId := uuid.New().String()
	// Get the query parameters
	bucketName, noOfMapperInstances, noOfMapperInstancesInt, noOfReducerInstances := getQueryParams(w, r)
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

	// Split the slice into DEFAULT_NO_OF_MAPPER_INSTANCES slices
	splitFiles := make([][]string, noOfMapperInstancesInt)
	for i, file := range files {
		splitFiles[i%noOfMapperInstancesInt] = append(splitFiles[i%noOfMapperInstancesInt], file)
	}
	// Send the slices to the splitter instances
	ctxBackground := context.Background()
	var wg sync.WaitGroup
	for i, files := range splitFiles {
		wg.Add(1)
		go sendToSplitter(ctxBackground, &wg, instanceId, bucketName, files, i, noOfMapperInstances, noOfReducerInstances)
	}
	wg.Wait()
	writeResponse(w, http.StatusOK, "MapReduce started successfully")
}

func getQueryParams(w http.ResponseWriter, r *http.Request) (bucketName string, noOfMapperInstances string,
	noOfMapperInstancesInt int, noOfReducerInstances string) {
	// Read bucket name from request
	bucketName = r.URL.Query().Get("bucket")
	// Read number of mapper instances from request
	noOfMapperInstances = r.URL.Query().Get("mappers")
	noOfMapperInstancesInt, err := strconv.Atoi(noOfMapperInstances)
	if err != nil && noOfMapperInstances != "" {
		writeResponse(w, http.StatusBadRequest, "Invalid number of mapper instances")
		return
	}
	if noOfMapperInstances == "" {
		noOfMapperInstances = strconv.Itoa(DEFAULT_NO_OF_MAPPER_INSTANCES)
	} else if noOfMapperInstancesInt < 1 || noOfMapperInstancesInt > 10 {
		writeResponse(w, http.StatusBadRequest, "Number of mapper instances must be between 1 and 10 (inclusive)")
		return
	}
	// Read number of reducer instances from request
	noOfReducerInstances = r.URL.Query().Get("reducers")
	noOfReducerInstancesInt, err := strconv.Atoi(noOfReducerInstances)
	if err != nil && noOfReducerInstances != "" {
		writeResponse(w, http.StatusBadRequest, "Invalid number of reducer instances")
		return
	}
	if noOfReducerInstances == "" {
		noOfReducerInstances = strconv.Itoa(DEFAULT_NO_OF_REDUCER_INSTANCES)
	} else if noOfReducerInstancesInt < 1 || noOfReducerInstancesInt > 10 {
		writeResponse(w, http.StatusBadRequest, "Number of reducer instances must be between 1 and 10 (inclusive)")
		return
	}
	return bucketName, noOfMapperInstances, noOfMapperInstancesInt, noOfReducerInstances
}

func sendToSplitter(ctx context.Context, wg *sync.WaitGroup, instanceId string, bucketName string, files []string,
	instanceNo int, noOfMappers string, noOfReducers string) {
	defer wg.Done()
	// Create a new pubsub client
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		log.Printf("Error creating client: %v", err)
		return
	}
	defer client.Close()
	// Set the topic the client will publish to
	mapperNo := strconv.Itoa(instanceNo)
	topic := client.Topic("mapreduce-splitter-" + mapperNo)
	// Create the struct to be sent to the splitter
	splitterData := SplitterData{
		BucketName: bucketName,
		FileNames:  files,
	}
	// Marshal the data to an array of bytes
	splitterDataBytes, err := json.Marshal(splitterData)
	if err != nil {
		log.Printf("Error marshalling splitter data: %v", err)
		return
	}
	// Publish the message to the topic
	result := topic.Publish(ctx, &pubsub.Message{
		Data: splitterDataBytes,
		Attributes: map[string]string{"instanceId": instanceId, "noOfMappers": noOfMappers,
			"noOfReducers": noOfReducers, "mapper": mapperNo},
	})
	// Get the result of the publish
	id, err := result.Get(ctx)
	if err != nil {
		log.Printf("Error publishing message to topic %s: %v", topic, err)
		return
	}
	log.Printf("Published a message to topic mapreduce-splitter-%s; msg ID: %v", strconv.Itoa(instanceNo), id)
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
