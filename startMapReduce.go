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
	"strconv"
	"strings"
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
	topic := client.Topic("mapreduce-splitter-" + strconv.Itoa(instanceNo))
	for _, file := range files {
		// Read the contents of the file
		data, err := readFileFromBucket(ctx, bucketName, file)
		if err != nil {
			log.Printf("Error reading file %s from bucket %s: %v", file, bucketName, err)
		}
		// Send the contents to the splitter instance
		result := topic.Publish(ctx, &pubsub.Message{
			Data:        data,
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
	//TODO: Need to remove stopwords and punctuation
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
	return processText(data), nil
}

func processText(data []byte) []byte {
	// Create a map containing all the stopwords as keys since Golang doesn't have sets
	stopwords := map[string]struct{}{"'tis": {}, "'twas": {}, "a": {}, "able": {}, "about": {}, "across": {},
		"after": {}, "ain't": {}, "all": {}, "almost": {}, "also": {}, "am": {}, "among": {}, "an": {}, "and": {},
		"any": {}, "are": {}, "aren't": {}, "as": {}, "at": {}, "be": {}, "because": {}, "been": {}, "but": {},
		"by": {}, "can": {}, "can't": {}, "cannot": {}, "could": {}, "could've": {}, "couldn't": {}, "dear": {},
		"did": {}, "didn't": {}, "do": {}, "does": {}, "doesn't": {}, "don't": {}, "either": {}, "else": {}, "ever": {},
		"every": {}, "for": {}, "from": {}, "get": {}, "got": {}, "had": {}, "has": {}, "hasn't": {}, "have": {},
		"he": {}, "he'd": {}, "he'll": {}, "he's": {}, "her": {}, "hers": {}, "him": {}, "his": {}, "how": {},
		"how'd": {}, "how'll": {}, "how's": {}, "however": {}, "i": {}, "i'd": {}, "i'll": {}, "i'm": {}, "i've": {},
		"if": {}, "in": {}, "into": {}, "is": {}, "isn't": {}, "it": {}, "it's": {}, "its": {}, "just": {}, "least": {},
		"let": {}, "like": {}, "likely": {}, "may": {}, "me": {}, "might": {}, "might've": {}, "mightn't": {},
		"most": {}, "must": {}, "must've": {}, "mustn't": {}, "my": {}, "neither": {}, "no": {}, "nor": {}, "not": {},
		"of": {}, "off": {}, "often": {}, "on": {}, "only": {}, "or": {}, "other": {}, "our": {}, "own": {},
		"rather": {}, "said": {}, "say": {}, "says": {}, "shan't": {}, "she": {}, "she'd": {}, "she'll": {},
		"she's": {}, "should": {}, "should've": {}, "shouldn't": {}, "since": {}, "so": {}, "some": {}, "than": {},
		"that": {}, "that'll": {}, "that's": {}, "the": {}, "their": {}, "them": {}, "then": {}, "there": {},
		"there's": {}, "these": {}, "they": {}, "they'd": {}, "they'll": {}, "they're": {}, "they've": {}, "this": {},
		"tis": {}, "to": {}, "too": {}, "twas": {}, "us": {}, "wants": {}, "was": {}, "wasn't": {}, "we": {},
		"we'd": {}, "we'll": {}, "we're": {}, "were": {}, "weren't": {}, "what": {}, "what'd": {}, "what's": {},
		"when": {}, "when'd": {}, "when'll": {}, "when's": {}, "where": {}, "where'd": {}, "where'll": {},
		"where's": {}, "which": {}, "while": {}, "who": {}, "who'd": {}, "who'll": {}, "who's": {}, "whom": {},
		"why": {}, "why'd": {}, "why'll": {}, "why's": {}, "will": {}, "with": {}, "won't": {}, "would": {},
		"would've": {}, "wouldn't": {}, "yet": {}, "you": {}, "you'd": {}, "you'll": {}, "you're": {}, "you've": {},
		"your": {},
	}
	text := string(data)
	words := strings.Fields(text)
	processedText := make([]string, 0)
	for i := 0; i < len(words); i++ {
		// Convert to lowercase
		words[i] = strings.ToLower(words[i])
		// Remove stopwords
		if _, ok := stopwords[words[i]]; ok {
			words[i] = ""
		}
		// Remove punctuation
		words[i] = strings.Trim(words[i], ".,;:!?\" ")
		// Remove empty strings
		if words[i] != "" {
			processedText = append(processedText, words[i])
		}
	}
	return []byte(strings.Join(processedText, " "))
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
