package map_phase

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce"
	"log"
	"sync"
	"testing"
	"time"
)

func TestSplitter(t *testing.T) {
	// Setup test
	teardown, subscriptions := serverless_mapreduce.SetupTest(t, []string{"mapreduce-mapper", "mapreduce-controller"})
	defer teardown(t)
	teardownTestStorage := serverless_mapreduce.CreateTestStorage(t)
	defer teardownTestStorage(t)

	// Given
	// Create a message
	inputData := serverless_mapreduce.SplitterData{
		BucketName: serverless_mapreduce.INPUT_BUCKET_NAME,
		FileName:   "test.txt",
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling splitter data: %v", err)
	}
	message := serverless_mapreduce.MessagePublishedData{
		Message: serverless_mapreduce.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []string{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog."}
	expectedControllerResult := serverless_mapreduce.StatusMessage{Status: serverless_mapreduce.STATUS_STARTED}

	// When
	err = splitter(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// Ensure the mapper received the correct data
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []string
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		msg.Ack()
	})
	// Ensure the message data matches the expected result
	assert.Equal(t, expectedResult, actualResult)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)

	// Ensure the controller received the correct message
	// The subscription will listen forever unless given a context with a timeout
	controllerCtx, controllerCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer controllerCancel()
	var received serverless_mapreduce.StatusMessage
	err = subscriptions[1].Receive(controllerCtx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &received)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		msg.Ack()
	})
	// Ensure the message data matches the expected result
	assert.Equal(t, expectedControllerResult.Status, received.Status)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}

func TestRemoveBookHeaderAndFooter(t *testing.T) {
	// Given
	inputText := []byte(`#SOME BOOK HEADER# *** START OF THIS PROJECT GUTENBERG EBOOK SOME TITLE *** The quick brown fox jumps over the lazy dog.
*** END OF THE PROJECT GUTENBERG EBOOK SOME TITLE *** #SOME BOOK FOOTER#`)
	expectedResult := []byte(`The quick brown fox jumps over the lazy dog.
`)

	// When
	actualResult := removeBookHeaderAndFooter(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestT(t *testing.T) {
	m := make(map[string]string)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			a := make(map[string]string)
			for k, v := range m {
				a[k] = v
			}
			a["id"] = uuid.New().String()
			time.Sleep(1 * time.Second)
			log.Println(a["id"])
			wg.Done()
		}()
	}
	wg.Wait()
}
