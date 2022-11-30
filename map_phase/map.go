package map_phase

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// Mapper is a function that is triggered by a message being published to the Mapper topic. It reads the split text from
// the message, pre-processes it, creates a key-value pair of the sorted word and the word and sends the list of key-value
// pairs to the combiner. It requires the message data to be of type []string.
func Mapper(ctx context.Context, e event.Event) error {
	start := time.Now()
	// Create a new pubsub client
	pubsubClient, err := pubsub.New(ctx, e)
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	// Read the data from the event i.e. message pushed from splitter
	var text []string
	attributes, err := pubsubClient.ReadPubSubMessage(&text)
	if err != nil {
		return err
	}

	// Map the words to their sorted form concurrently and store the results in mappedText
	var mappedText []pubsub.MapperData
	var wg sync.WaitGroup
	// Create a buffered channel to store the key-value pairs
	keyValueChan := make(chan pubsub.MapperData, 1000)
	go func() {
		defer close(keyValueChan)
		// Iterate over the words in the text and map them to their sorted form
		for _, word := range text {
			wg.Add(1)
			// Map each word in a goroutine
			go mapWord(&wg, keyValueChan, word)
		}
		// Wait for all the text to be mapped
		wg.Wait()
	}()
	// Read the key-value pairs from the channel and append them to mappedText
	for wordData := range keyValueChan {
		mappedText = append(mappedText, wordData)
	}
	// Create a client for the combine topic
	//topic := client.Topic(tools.COMBINE_TOPIC)
	//defer topic.Stop()
	// Send one pubsub message to the combiner per book to reduce the number of invocations -> reduce cost
	pubsubClient.SendPubSubMessage(pubsub.COMBINE_TOPIC, mappedText, attributes)
	log.Printf("Mapper took %v to run", time.Since(start))
	return nil
}

// mapWord maps a word to its sorted form and stores the result in mappedText. It accepts a pointer to a WaitGroup, a
// pointer to a sync.Mutex, a pointer to a slice of MapperData and a string. It returns nothing.
func mapWord(wg *sync.WaitGroup, keyValueChan chan pubsub.MapperData, word string) {
	defer wg.Done()
	// Do some preprocessing on the word
	preProcessedWord := preProcessWord(word)
	// If the word is empty after preprocessing, return early
	if preProcessedWord == "" {
		return
	}
	// sort string into alphabetical order
	splitWord := strings.Split(preProcessedWord, "")
	sort.Strings(splitWord)
	sortedWord := strings.Join(splitWord, "")
	// Use a map as the value in the key-value pair to avoid duplicates in later stages
	anagrams := make(map[string]struct{})
	// Add the word to the map with an empty struct as the value to save memory
	anagrams[preProcessedWord] = struct{}{}
	// Write the key-value pair to the channel
	keyValueChan <- pubsub.MapperData{SortedWord: sortedWord, Anagrams: anagrams}
}

// preProcessWord receives a lowercase word and strips any punctuation from the word. It also removes a word if it is
// a stop word or contains any numbers or symbols.
func preProcessWord(word string) string {
	// Use a map to replicate the functionality of a set since Go doesn't have a set data structure
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
	// Remove any punctuation from the start and end of the word
	word = strings.Trim(word, ".,;:!?()'\" ")
	// Remove the word if it is a stopword or contains numbers or symbols
	if _, ok := stopwords[word]; ok || strings.ContainsAny(word, "0123456789*+-_&^%$#@!~`|}{[]\\:;\"'<>,.?/()") {
		return ""
	}
	return word
}
