package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

const BUCKET_NAME = "test-bucket"

func createTestStorage(tb testing.TB) func(tb testing.TB) {
	// Setup test
	ctx := context.Background()
	// Modify the STORAGE_EMULATOR_HOST environment variable to point to the storage emulator
	existingStorageVal := os.Getenv("STORAGE_EMULATOR_HOST")
	err := os.Setenv("STORAGE_EMULATOR_HOST", "localhost:9023")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a storage client so we can create a bucket
	client, err := storage.NewClient(ctx)
	if err != nil {
		tb.Fatalf("Error creating storage client: %v", err)
	}
	bucket := client.Bucket(BUCKET_NAME)
	if err := bucket.Create(ctx, "serverless-mapreduce", nil); err != nil {
		tb.Fatalf("Error creating bucket: %v", err)
	}
	// Create a file in the bucket
	object := bucket.Object("test.txt")
	writer := object.NewWriter(ctx)
	if _, err := writer.Write([]byte("#This text will be removed# START OF THIS PROJECT GUTENBERG EBOOK The quick " +
		"brown fox jumps over the lazy dog.")); err != nil {
		tb.Fatalf("Error writing to bucket: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("Error closing bucket: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		// Delete the file in the bucket
		if err := object.Delete(ctx); err != nil {
			tb.Fatalf("Error deleting object: %v", err)
		}
		// Delete the bucket
		if err := bucket.Delete(ctx); err != nil {
			tb.Fatalf("Error deleting bucket: %v", err)
		}
		// Reset the STORAGE_EMULATOR_HOST environment variable
		err = os.Setenv("STORAGE_EMULATOR_HOST", existingStorageVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}
}

func TestStartMapReduce(t *testing.T) {
	teardown, subscription := setupTest(t, "mapreduce-splitter-0")
	defer teardown(t)
	teardown2 := createTestStorage(t)
	defer teardown2(t)

	// Given
	req := httptest.NewRequest(http.MethodGet, "https://someurl.com?bucket="+BUCKET_NAME, nil)
	rec := httptest.NewRecorder()

	expectedResponse := `{"responseCode":200,"message":"MapReduce started successfully"}`
	expectedResult := []byte("The quick brown fox jumps over the lazy dog.")

	// When
	startMapreduce(rec, req)

	// Then
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedResponse, rec.Body.String())
	// The subscription will listen forever unless given a context with a timeout
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	var actualResult []byte
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Ensure the message data matches the expected result
		actualResult = msg.Data
		msg.Ack()
	})
	assert.Equal(t, expectedResult, actualResult)
	// Ensure there are no errors returned by the receiver
	assert.Nil(t, err)
}

// TODO: Create tests for other responses

func TestRemoveBookHeaderAndFooter(t *testing.T) {
	// Given
	inputText := []byte(`#SOME BOOK HEADER# *** START OF THIS PROJECT GUTENBERG EBOOK SOME TITLE *** The quick brown fox jumps over the lazy dog.
*** END OF THE PROJECT GUTENBERG EBOOK SOME TITLE *** #SOME BOOK FOOTER#`)
	expectedResult := []byte(`The quick brown fox jumps over the lazy dog.
`)

	// When
	actualResult := removeBookHeaderAndFooter(inputText)

	// Then
	assert.Equal(t, expectedResult, actualResult)
}
