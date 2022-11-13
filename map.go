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
	for _, word := range text.Text {
		wg.Add(1)
		go sendToReducer(ctx, &wg, msg.Message.Attributes["instanceId"], client, word)
	}
	wg.Wait()
	return nil
}

func sendToReducer(ctx context.Context, wg *sync.WaitGroup, instanceId string, client *pubsub.Client, word string) {
	// sort string into alphabetical order
	splitWord := strings.Split(word, "")
	sort.Strings(splitWord)
	sortedWord := strings.Join(splitWord, "")
	wordData := WordData{
		SortedWord: sortedWord,
		Word:       word,
	}
	// Marshal wordData into JSON to be sent to reducer
	wordDataJson, err := json.Marshal(wordData)
	if err != nil {
		log.Printf("Error marshalling wordData: %v", err)
	}
	// send to reducer
	reducerNum := findReducerNum(sortedWord)
	topic := client.Topic("mapreduce-shuffler-" + reducerNum)
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        wordDataJson,
		Attributes:  map[string]string{"instanceId": instanceId},
		PublishTime: time.Now(),
	})
	_, err = result.Get(ctx)
	if err != nil {
		log.Printf("Error sending to shuffler: %v", err)
	}
	log.Printf("Sent %s : %s to shuffler %s", sortedWord, word, reducerNum)
	wg.Done()
}

func findReducerNum(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	hashedString := h.Sum32()
	return strconv.Itoa(int(hashedString % 1))
}
