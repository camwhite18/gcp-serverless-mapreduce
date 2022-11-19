package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http/httptest"
	"os"
	"testing"
)

func SetupTest(tb testing.TB, wordData WordData) func(tb testing.TB) {
	// Setup test
	// Modify the REDIS_HOST environment variable to point to the pubsub emulator
	existingRedisHostVal := os.Getenv("REDIS_HOST")
	err := os.Setenv("REDIS_HOST", "localhost")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Modify the REDIS_PORT environment variable to point to the pubsub emulator
	existingRedisPortVal := os.Getenv("REDIS_PORT")
	err = os.Setenv("REDIS_PORT", "6379")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}

	// Connect to redis
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)
	const maxConnections = 10
	redisPool = &redis.Pool{
		MaxIdle: maxConnections,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddress)
		},
	}

	return func(tb testing.TB) {
		// Teardown test
		conn := redisPool.Get()
		defer conn.Close()
		_, err := conn.Do("DEL", wordData.SortedWord)
		if err != nil {
			tb.Fatalf("Error getting data from redis: %v", err)
		}
		// Reset the REDIS_HOST environment variable
		err = os.Setenv("REDIS_HOST", existingRedisHostVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
		// Reset the REDIS_PORT environment variable
		err = os.Setenv("REDIS_PORT", existingRedisPortVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}
}

func TestShuffler(t *testing.T) {
	// Given
	wordData := WordData{
		Word:       "quick",
		SortedWord: "ciuqk",
	}
	teardown := SetupTest(t, wordData)
	defer teardown(t)
	// Create a message
	wordDataBytes, err := json.Marshal(wordData)
	if err != nil {
		t.Fatalf("Error marshalling word data: %v", err)
	}
	message := MessagePublishedData{
		Message: PubSubMessage{
			Data:       wordDataBytes,
			Attributes: map[string]string{"splitter": "0", "noOfReducers": "1"},
		},
	}

	req := httptest.NewRequest("GET", "http://localhost:8080", nil)
	val, _ := json.Marshal(message)
	req.Body = io.NopCloser(bytes.NewReader(val))
	rec := httptest.NewRecorder()

	expectedResult := []string{"quick"}

	// When
	shuffle(rec, req)

	// Then
	conn := redisPool.Get()
	defer conn.Close()
	li, err := conn.Do("GET", wordData.SortedWord)
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	err = json.Unmarshal(li.([]byte), &actualResult)
	if err != nil {
		t.Fatalf("Error marshalling data to redis: %v", err)
	}
	assert.Equal(t, expectedResult, actualResult)
}
