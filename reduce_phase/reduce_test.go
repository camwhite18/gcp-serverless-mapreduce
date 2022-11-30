package reduce_phase

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/tools"
	"io"
	"testing"
	"time"
)

func TestReducer(t *testing.T) {
	// Given
	teardown, _ := tools.SetupTest(t, []string{})
	defer teardown(t)
	teardownStorage := tools.CreateTestStorage(t)
	defer teardownStorage(t)
	teardownRedis := tools.SetupRedisTest(t)
	defer teardownRedis(t)

	inputDataBytes, err := json.Marshal(nil)
	if err != nil {
		t.Fatalf("Error marshalling mapper data: %v", err)
	}
	message := tools.MessagePublishedData{
		Message: tools.PubSubMessage{
			Data:       inputDataBytes,
			Attributes: map[string]string{"outputBucket": tools.OUTPUT_BUCKET_NAME, "reducerNum": "1"},
		},
	}
	// Create a CloudEvent to be sent to the mapper
	e := event.New()
	e.SetDataContentType("application/json")
	err = e.SetData(e.DataContentType(), message)
	if err != nil {
		t.Fatalf("Error setting event data: %v", err)
	}

	expectedResult1 := "acer: care race\n"
	expectedResult2 := "aprt: part trap\n"

	// When
	err = Reducer(context.Background(), e)

	// Then
	assert.Nil(t, err)
	// Check that the data was stored in the file correctly
	storageCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	// Create a storage client so we can create a bucket
	client, err := storage.NewClient(storageCtx)
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	// Create a reader to read the file
	reader, err := client.Bucket(tools.OUTPUT_BUCKET_NAME).Object("anagrams-part-1.txt").NewReader(storageCtx)
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}
	// Read the file
	actualResult, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	// Check that the data is correct
	assert.Contains(t, string(actualResult), expectedResult1)
	assert.Contains(t, string(actualResult), expectedResult2)
}
