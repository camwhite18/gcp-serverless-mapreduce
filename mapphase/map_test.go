package mapphase

import (
	ps "cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"log"
	"testing"
	"time"
)

func TestMapper(t *testing.T) {
	// Setup test
	teardown, subscriptions := test.SetupPubSubTest(t, []string{pubsub.CombineTopic})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []string{"the", "quick", "brown", "fox", "quick"}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []pubsub.MappedWord{
		{Anagrams: map[string]struct{}{"quick": {}}, SortedWord: "cikqu"},
		{Anagrams: map[string]struct{}{"brown": {}}, SortedWord: "bnorw"},
		{Anagrams: map[string]struct{}{"fox": {}}, SortedWord: "fox"},
	}

	// When
	err = Mapper(context.Background(), e)

	// Then
	// Ensure there are no errors returned
	assert.Nil(t, err)
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult []pubsub.MappedWord
	err = subscriptions[0].Receive(ctx, func(ctx context.Context, msg *ps.Message) {
		// Unmarshal the message data into the MappedWord struct
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

func TestMapper_ReadPubSubMessageError(t *testing.T) {
	// Setup test
	teardown, _ := test.SetupPubSubTest(t, []string{pubsub.CombineTopic})
	defer teardown(t)
	// Given
	// Create a message
	inputData := []int{1, 2, 3}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Mapper(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error unmarshalling message")
}

func TestMapper_CreatePubSubClientError(t *testing.T) {
	// Given
	// Create a message
	inputData := []string{"the", "quick", "brown", "fox", "quick"}
	inputDataBytes, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("Error marshalling Mapper data: %v", err)
	}
	message := pubsub.MessagePublishedData{
		Message: pubsub.Message{
			Data:       inputDataBytes,
			Attributes: make(map[string]string),
		},
	}
	// Create a CloudEvent to be sent to the Mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	// When
	err = Mapper(context.Background(), e)

	// Then
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "error creating pubsub client")
}

func TestProcessText(t *testing.T) {
	// Given
	inputText := "teststring."
	expectedResult := "teststring"

	// When
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextNumber(t *testing.T) {
	// Given
	inputText := "test1string"
	expectedResult := ""

	// When
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestProcessTextStopWord(t *testing.T) {
	// Given
	inputText := "would've"
	expectedResult := ""

	// When
	actualResult := preProcessWord(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}

func TestTrimNonAlphabetic(t *testing.T) {
	// Given
	inputText := ";'.[]/,'][çteststringç];/];];/'"
	expectedResult := "çteststringç"
	str := "test"
	log.Printf("w: %s", str[:1])

	// When
	actualResult := trimNonAlphabeticCharacters(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}
