package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	functions.CloudEvent("Mapper", mapper)
}

func mapper(ctx context.Context, e event.Event) error {
	startTime := time.Now()
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("error getting data from event: %v", err)
	}
	text := MapperData{}
	if err := json.Unmarshal(msg.Message.Data, &text); err != nil {
		return fmt.Errorf("error unmarshalling message: %v", err)
	}
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		return fmt.Errorf("error creating pubsub client: %v", err)
	}
	defer client.Close()
	var wg sync.WaitGroup
	reducerWordMap := makeWordMap(text.Text, msg.Message.Attributes["noOfReducers"])
	for reducerNum, wordData := range reducerWordMap {
		wg.Add(1)
		go sendToShuffler(ctx, &wg, msg.Message.Attributes, reducerNum, wordData, client)
	}
	wg.Wait()
	finishTime := time.Now()
	log.Printf("Mapper took %v to run", finishTime.Sub(startTime))
	return nil
}

func makeWordMap(text []string, noOfReducers string) map[string][]WordData {
	wordMap := make(map[string][]WordData)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, word := range text {
		wg.Add(1)
		word := word
		go func() {
			defer wg.Done()
			processedWord := processText(word)
			if processedWord == "" {
				return
			}
			// sort string into alphabetical order
			splitWord := strings.Split(processedWord, "")
			sort.Strings(splitWord)
			sortedWord := strings.Join(splitWord, "")
			reducerNum, err := partition(sortedWord, noOfReducers)
			if err != nil {
				log.Printf("Error finding reducer number: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if wordMap[reducerNum] == nil {
				wordMap[reducerNum] = make([]WordData, 0)
			}
			wordMap[reducerNum] = append(wordMap[reducerNum], WordData{
				SortedWord: sortedWord,
				Word:       processedWord,
			})
		}()
	}
	wg.Wait()
	return wordMap
}

func partition(s string, noOfReducers string) (string, error) {
	h := fnv.New32a()
	h.Write([]byte(s))
	hashedString := h.Sum32()
	noOfReducersInt, err := strconv.Atoi(noOfReducers)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(int(hashedString % uint32(noOfReducersInt))), nil
}

func sendToShuffler(ctx context.Context, wg *sync.WaitGroup, attributes map[string]string, reducerNum string, wordData []WordData, client *pubsub.Client) {
	defer wg.Done()
	topic := client.Topic("mapreduce-shuffler-" + reducerNum)
	topic.PublishSettings.ByteThreshold = MAX_MESSAGE_SIZE_BYTES
	topic.PublishSettings.CountThreshold = MAX_MESSAGE_COUNT
	topic.PublishSettings.DelayThreshold = MAX_MESSAGE_DELAY
	data, err := json.Marshal(wordData)
	if err != nil {
		log.Printf("Error marshalling word data: %v", err)
		return
	}
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        data,
		Attributes:  attributes,
		PublishTime: time.Now(),
	})
	_, err = result.Get(ctx)
	if err != nil {
		log.Printf("Error publishing message: %v", err)
	}
}

func processText(word string) string {
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
	// Convert to lowercase
	word = strings.ToLower(word)
	// Remove punctuation
	word = strings.Trim(word, ".,;:!?\" ")
	// Remove the word if it is a stopword or contains numbers or symbols
	if _, ok := stopwords[word]; ok || strings.ContainsAny(word, "0123456789*+_&^%$#@!~`|}{[]\\:;\"'<>,.?/") {
		return ""
	}
	return word
}
