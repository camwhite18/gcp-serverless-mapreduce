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

func TestMapper(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-shuffler-0")
	defer teardown(t)
	// Given
	// Create a message
	inputData := []string{"quick", "brown", "fox", "quick"}
	mapperData := MapperData{
		Text: inputData,
	}
	mapperDataBytes, err := json.Marshal(mapperData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       mapperDataBytes,
			Attributes: map[string]string{"splitter": "0", "noOfReducers": "1"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []WordData{
		{Word: "quick", SortedWord: "cikqu"},
		{Word: "brown", SortedWord: "bnorw"},
		{Word: "fox", SortedWord: "fox"},
		{Word: "quick", SortedWord: "cikqu"},
	}

	// When
	err = mapper(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []WordData
	err = subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Unmarshal the message data into the WordData struct
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		msg.Ack()
	})
	// Ensure the message data matches the expected result
	for i := 0; i < len(expectedResult); i++ {
		assert.Contains(t, actualResult, expectedResult[i])
	}
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}

func TestProcessText(t *testing.T) {
	// Given
	inputText := "TestString."
	expectedResult := "teststring"

	// When
	actualResult := processText(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextNumber(t *testing.T) {
	// Given
	inputText := "Test1String"
	expectedResult := ""

	// When
	actualResult := processText(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextStopWord(t *testing.T) {
	// Given
	inputText := "Would've"
	expectedResult := ""

	// When
	actualResult := processText(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestMakeWordMap(t *testing.T) {
	// Given
	inputText := []string{"quick", "brown", "fox", "quick"}
	noOfReducers := "2"

	expectedResult := map[string][]WordData{
		"0": {
			{Word: "quick", SortedWord: "cikqu"},
			{Word: "fox", SortedWord: "fox"},
			{Word: "quick", SortedWord: "cikqu"},
		},
		"1": {
			{Word: "brown", SortedWord: "bnorw"},
		},
	}

	// When
	actualResult := makeWordMap(inputText, noOfReducers)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}
