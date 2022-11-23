package serverless_mapreduce

import (
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"sort"
	"strings"
	"sync"
)

func init() {
	functions.CloudEvent("Mapper", mapper)
}

func mapper(ctx context.Context, e event.Event) error {
	var text []string
	client, _, err := ReadPubSubMessage(ctx, e, &text)
	if err != nil {
		return err
	}
	defer client.Close()
	var mappedText []WordData
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, word := range text {
		wg.Add(1)
		go mapWord(&wg, &mu, &mappedText, word)
	}
	wg.Wait()
	// Send the mapped text to the shuffler
	topic := client.Topic("mapreduce-shuffler")
	defer topic.Stop()
	SendPubSubMessage(ctx, nil, topic, mappedText, nil)
	return nil
}

func mapWord(wg *sync.WaitGroup, mu *sync.Mutex, mappedText *[]WordData, word string) {
	defer wg.Done()
	preProcessedWord := preProcessWord(word)
	if preProcessedWord == "" {
		return
	}
	// sort string into alphabetical order
	splitWord := strings.Split(preProcessedWord, "")
	sort.Strings(splitWord)
	sortedWord := strings.Join(splitWord, "")
	mu.Lock()
	*mappedText = append(*mappedText, WordData{SortedWord: sortedWord, Word: preProcessedWord})
	mu.Unlock()
}

func preProcessWord(word string) string {
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
	if _, ok := stopwords[word]; ok || strings.ContainsAny(word, "0123456789*+-_&^%$#@!~`|}{[]\\:;\"'<>,.?/") {
		return ""
	}
	return word
}
