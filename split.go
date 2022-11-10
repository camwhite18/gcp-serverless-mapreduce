package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
	"strings"
	"time"
)

func init() {
	functions.CloudEvent("Splitter", splitter)
}

func splitter(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("error getting data from event: %v", err)
	}
	mapperData := MapperData{
		Text: processText(msg.Message.Data),
	}
	mapperDataMarshalled, err := json.Marshal(mapperData)
	if err != nil {
		return fmt.Errorf("error marshalling mapper data: %v", err)
	}
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		log.Printf("Error creating client: %v", err)
	}
	defer client.Close()
	// Set the topic the client will publish to
	topic := client.Topic("mapreduce-mapper-" + msg.Message.Attributes["splitter"])
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        mapperDataMarshalled,
		PublishTime: time.Now(),
	})
	// Get the result of the publish
	id, err := result.Get(ctx)
	if err != nil {
		log.Printf("Error publishing message to topic %s: %v", topic, err)
	}
	log.Printf("Published a message to topic mapreduce-splitter-%s; msg ID: %v", msg.Message.Attributes["splitter"], id)
	return nil
}

func processText(data []byte) []string {
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
		// Remove stopwords and words containing numbers
		if _, ok := stopwords[words[i]]; ok || strings.ContainsAny(words[i], "0123456789") {
			words[i] = ""
		}
		// Remove punctuation
		words[i] = strings.Trim(words[i], ".,;:!?\" ")
		// Remove empty strings
		if words[i] != "" {
			processedText = append(processedText, words[i])
		}
	}
	return processedText
}
