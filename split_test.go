package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSplitter(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-mapper-0")
	defer teardown(t)
	teardownTestStorage := createTestStorage(t)
	defer teardownTestStorage(t)

	// Given
	// Create a message
	inputData := SplitterData{
		BucketName: BUCKET_NAME,
		FileNames:  []string{"test.txt"},
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling splitter data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       inputDataBytes,
			Attributes: map[string]string{"instanceId": "12345", "noOfMappers": "1", "noOfReducers": "1", "mapper": "0"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := MapperData{
		Text: []string{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog."},
	}

	// When
	err = splitter(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult MapperData
	err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
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
