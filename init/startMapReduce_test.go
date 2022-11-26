package init

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStartMapReduce(t *testing.T) {
	teardown, subscriptions := serverless_mapreduce.SetupTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)
	teardownTestStorage := serverless_mapreduce.CreateTestStorage(t)
	defer teardownTestStorage(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, "https://someurl.com?bucket="+serverless_mapreduce.INPUT_BUCKET_NAME, nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":200,"message":"MapReduce started successfully"}`
	expectedResult := serverless_mapreduce.SplitterData{
		BucketName: serverless_mapreduce.INPUT_BUCKET_NAME,
		FileName:   "test.txt",
	}

	// When
	startMapreduce(rec, req)

	// Then
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult serverless_mapreduce.SplitterData
	err := subscriptions[0].Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
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
