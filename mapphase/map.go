package mapphase

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Mapper is a function that is triggered by a message being published to the Mapper topic. It reads the split text from
// the message, pre-processes it, creates a key-value pair of the sorted word and the original word and sends the list
// of key-value pairs for the received partition to the Combiner. It requires the message data to be of type []string.
func Mapper(ctx context.Context, e event.Event) error {
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
	var mappedText []pubsub.MappedWord
	var wg sync.WaitGroup
	// Create a buffered channel to store the key-value pairs
	keyValueChan := make(chan pubsub.MappedWord, 1000)
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
	// Send one pubsub message to the combiner per book to reduce the number of invocations -> reduce cost
	pubsubClient.SendPubSubMessage(pubsub.CombineTopic, mappedText, attributes)
	return nil
}

// mapWord maps a word to its sorted form and pushed the result onto the keyValue channel. If the word is empty after
// pre-processing, it is discounted. It accepts a pointer to a WaitGroup, a pointer to a sync.Mutex, a pointer to a
// slice of MappedWord and a string. It returns nothing.
func mapWord(wg *sync.WaitGroup, keyValueChan chan pubsub.MappedWord, word string) {
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
	// Use a map as the value in the key-value pair to avoid duplicates in later stages (sets don't exist in Go)
	// Add the word to the map with an empty struct as the value to save memory
	anagrams := map[string]struct{}{preProcessedWord: {}}
	// Write the key-value pair to the channel
	keyValueChan <- pubsub.MappedWord{SortedWord: sortedWord, Anagrams: anagrams}
}

// preProcessWord receives a lowercase word and strips any non-alphabetic characters from the start and end of the word.
// If the word is a stop word, or still contains any non-alphabetic characters, it returns an empty string. Otherwise,
// it returns the pre-processed word.
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
	// Remove any punctuation and numbers from the start and end of the word
	numbersAndSymbols := "0123456789*+-_&^%$#@!~`|}{[]\\:;\"'<>,.?/()="
	word = strings.Trim(word, numbersAndSymbols)
	// Remove the word if it is a stop-word, or contains non-alphabetic characters
	if _, ok := stopwords[word]; ok || !containsOnlyLetters(word) {
		return ""
	}
	return word
}

// containsOnlyLetters returns true if the string contains only alphabetic characters, and false otherwise.
func containsOnlyLetters(word string) bool {
	for _, char := range word {
		if !unicode.IsLetter(char) {
			return false
		}
	}
	return true
}
