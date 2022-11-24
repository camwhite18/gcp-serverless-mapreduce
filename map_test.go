package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestMapper(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-combine")
	defer teardown(t)
	// Given
	// Create a message
	inputData := []string{"quick", "brown", "fox", "quick"}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data: inputDataBytes,
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
		{Anagrams: map[string]struct{}{"quick": {}}, SortedWord: "cikqu"},
		{Anagrams: map[string]struct{}{"brown": {}}, SortedWord: "bnorw"},
		{Anagrams: map[string]struct{}{"fox": {}}, SortedWord: "fox"},
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
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextNumber(t *testing.T) {
	// Given
	inputText := "Test1String"
	expectedResult := ""

	// When
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextStopWord(t *testing.T) {
	// Given
	inputText := "Would've"
	expectedResult := ""

	// When
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestMapperPerformance(t *testing.T) {
	// Setup test
	teardown, subscription := SetupTest(t, "mapreduce-combine")
	defer teardown(t)
	// Given
	// Create a message
	var inputData []string
	for i := 0; i < 100000; i++ {
		inputData = append(inputData, "quick")
	}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data: inputDataBytes,
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	var expectedResult []WordData
	for i := 0; i < 100000; i++ {
		expectedResult = append(expectedResult, WordData{Anagrams: map[string]struct{}{"quick": {}}, SortedWord: "cikqu"})
	}

	// When
	start := time.Now()
	err = mapper(context.Background(), e)
	log.Printf("Mapper took %v", time.Since(start))

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
	assert.Equal(t, expectedResult, actualResult)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}
