package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStartMapReduce(t *testing.T) {
	teardown, subscription := SetupTest(t, "mapreduce-splitter-0")
	defer teardown(t)
	teardownTestStorage := createTestStorage(t)
	defer teardownTestStorage(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, "https://someurl.com?mappers=1&bucket="+BUCKET_NAME, nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":200,"message":"MapReduce started successfully"}`
	expectedResult := SplitterData{
		BucketName: BUCKET_NAME,
		FileNames:  []string{"test.txt"},
	}

	// When
	startMapreduce(rec, req)

	// Then
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult SplitterData
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Ensure the message data matches the expected result
		err := json.Unmarshal(msg.Data, &actualResult)
		if err != nil {
			t.Fatalf("Error unmarshalling message: %v", err)
		}
		msg.Ack()
	})
	assert.Equal(t, expectedResult, actualResult)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}

// TODO: Create tests for other responses
