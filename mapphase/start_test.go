package mapphase

import (
	ps "cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/pubsub"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStartMapReduce(t *testing.T) {
	teardown, subscriptions := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)
	teardownTestStorage := test.SetupStorageTest(t)
	defer teardownTestStorage(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s&output-bucket=%s",
		test.InputBucketName, test.OutputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":200,"message":"MapReduce started successfully - results will be stored in: test-bucket-output"}`
	expectedResult := pubsub.SplitterData{
		BucketName: test.InputBucketName,
		FileName:   "test.txt",
	}

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
	// The subscription will listen forever unless given a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var actualResult pubsub.SplitterData
	err := subscriptions[0].Receive(ctx, func(ctx context.Context, msg *ps.Message) {
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

func TestStartMapReduce_CreatePubSubClientError(t *testing.T) {
	teardownTestStorage := test.SetupStorageTest(t)
	defer teardownTestStorage(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s&output-bucket=%s",
		test.InputBucketName, test.OutputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":500,"message":"`

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), expectedResponse)
}

func TestStartMapReduce_EmptyInputBucketError(t *testing.T) {
	teardown, _ := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)
	teardownTestStorage := test.SetupStorageTest(t)
	defer teardownTestStorage(t)

	// Given
	bucketName := "some-empty-bucket"
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s&output-bucket=%s",
		bucketName, test.OutputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":400,"message":"No files found in input bucket: some-empty-bucket"}`

	// Create an empty input bucket
	createStorageCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	client, err := storage.NewClient(createStorageCtx)
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	inputBucket := client.Bucket(bucketName)
	defer cancel()
	if err := inputBucket.Create(createStorageCtx, "serverless-mapreduce", nil); err != nil &&
		!strings.Contains(err.Error(), "already own this bucket") {
		t.Fatalf("Error creating inputBucket: %v", err)
	}

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
}

func TestStartMapReduce_StorageClientCreateError(t *testing.T) {
	teardown, _ := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s&output-bucket=%s",
		test.InputBucketName, test.OutputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":500,"message":"`

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), expectedResponse)
}

func TestStartMapReduce_InvalidInputBucketError(t *testing.T) {
	teardown, _ := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)
	teardownTestStorage := test.SetupStorageTest(t)
	defer teardownTestStorage(t)

	// Given
	bucketName := "some-invalid-bucket"
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s&output-bucket=%s",
		bucketName, test.OutputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":400,"message":"Storage bucket doesn't exist or isn't accessible"}`

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
}

func TestStartMapReduce_NoOutputBucketProvidedError(t *testing.T) {
	teardown, _ := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("https://someurl.com?input-bucket=%s",
		test.InputBucketName), nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":400,"message":"No output bucket name provided, please provide one using the query parameter 'output-bucket'"}`

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
}

func TestStartMapReduce_NoInputBucketProvidedError(t *testing.T) {
	teardown, _ := test.SetupPubSubTest(t, []string{"mapreduce-splitter"})
	defer teardown(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, "https://someurl.com", nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":400,"message":"No input bucket name provided, please provide one using the query parameter 'input-bucket'"}`

	// When
	StartMapReduce(rec, req)

	// Then
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
}
