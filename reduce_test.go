package serverless_mapreduce

import (
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestReducer(t *testing.T) {
	// Given
	teardown, _ := SetupTest(t, "mapreduce-reducer-0")
	defer teardown(t)
	teardownRedis := SetupRedisTest(t)
	defer teardownRedis(t)
	wordDataSlice := []WordData{
		{SortedWord: "acer", Anagrams: map[string]struct{}{"care": {}, "race": {}}},
	}
	// Create a message
	wordDataBytes, err := json.Marshal(wordDataSlice)
	if err != nil {
		t.Fatalf("Error marshalling word data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       wordDataBytes,
			Attributes: map[string]string{"splitter": "0", "noOfReducers": "1"},
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []string{"care", "race"}

	// When
	err = reducer(context.Background(), e)

	// Then
	assert.Nil(t, err)
	conn := redisPool.Get()
	defer conn.Close()

	li, err := conn.Do("SMEMBERS", wordDataSlice[0].SortedWord)
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	for _, v := range li.([]interface{}) {
		actualResult = append(actualResult, string(v.([]byte)))
	}
	assert.Equal(t, expectedResult, actualResult)
}

func TestReducerEfficiency(t *testing.T) {
	// Given
	teardown, _ := SetupTest(t, "mapreduce-reducer-0")
	defer teardown(t)
	teardownRedis := SetupRedisTest(t)
	defer teardownRedis(t)
	var wordDataSlice []WordData
	for i := 0; i < 10000; i++ {
		wordDataSlice = append(wordDataSlice, WordData{Anagrams: map[string]struct{}{"quick": {}}, SortedWord: "cikqu"})
	}
	// Create a message
	wordDataBytes, err := json.Marshal(wordDataSlice)
	if err != nil {
		t.Fatalf("Error marshalling word data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       wordDataBytes,
			Attributes: map[string]string{"splitter": "0", "noOfReducers": "1"},
		},
	}

	// Create a CloudEvent to be sent to the shuffler
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult := []string{"quick"}

	// When
	startTime := time.Now()
	err = reducer(context.Background(), e)
	elapsedTime := time.Since(startTime)
	log.Printf("Shuffler took %s", elapsedTime)

	// Then
	assert.Nil(t, err)
	conn := redisPool.Get()
	defer conn.Close()

	li, err := conn.Do("SMEMBERS", wordDataSlice[0].SortedWord)
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	for _, v := range li.([]interface{}) {
		actualResult = append(actualResult, string(v.([]byte)))
	}
	assert.Equal(t, expectedResult, actualResult)
}
