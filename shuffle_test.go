package serverless_mapreduce

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func SetupRedisTest(tb testing.TB) func(tb testing.TB) {
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
		_, err := conn.Do("FLUSHALL")
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
	teardown, _ := SetupTest(t, "mapreduce-reducer-0")
	defer teardown(t)
	teardownRedis := SetupRedisTest(t)
	defer teardownRedis(t)
	wordDataSlice := []WordData{
		{Word: "quick", SortedWord: "cikqu"},
		{Word: "brown", SortedWord: "bnorw"},
		{Word: "fox", SortedWord: "fox"},
		{Word: "quick", SortedWord: "cikqu"},
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

	expectedResult := []string{"quick", "quick"}

	// When
	err = shuffler(context.Background(), e)

	// Then
	assert.Nil(t, err)
	conn := redisPool.Get()
	defer conn.Close()

	li, err := conn.Do("LRANGE", wordDataSlice[0].SortedWord, 0, -1)
	if err != nil {
		t.Fatalf("Error getting data from redis: %v", err)
	}
	var actualResult []string
	for _, v := range li.([]interface{}) {
		actualResult = append(actualResult, string(v.([]byte)))
	}
	assert.Equal(t, expectedResult, actualResult)
}
