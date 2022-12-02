package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/stretchr/testify/assert"
	"gitlab.com/cameron_w20/serverless-mapreduce/test"
	"testing"
)

func TestReadObjectNames(t *testing.T) {
	// Setup test
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)

	// Given
	client, err := New(context.Background())
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	defer client.Close()

	// When
	objectNames, err := client.ReadObjectNames(context.Background(), test.INPUT_BUCKET_NAME)
	if err != nil {
		return
	}

	// Then
	assert.Nil(t, err)
	assert.Equal(t, []string{"test.txt"}, objectNames)
}

func TestReeadObjectNames_TxtFilesOnly(t *testing.T) {
	// Setup test
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)

	// Given
	client, err := New(context.Background())
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	defer client.Close()

	c, err := storage.NewClient(context.Background())
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	bucket := c.Bucket(test.INPUT_BUCKET_NAME)
	err = bucket.Object("test.txt").Delete(context.Background())
	if err != nil {
		t.Fatalf("Error deleting object: %v", err)
	}
	err = bucket.Object("test.csv").NewWriter(context.Background()).Close()
	if err != nil {
		t.Fatalf("Error creating object: %v", err)
	}

	// When
	objectNames, err := client.ReadObjectNames(context.Background(), test.INPUT_BUCKET_NAME)
	if err != nil {
		return
	}

	// Then
	assert.Nil(t, err)
	assert.Empty(t, objectNames)

	err = bucket.Object("test.csv").Delete(context.Background())
	if err != nil {
		t.Fatalf("Error deleting object: %v", err)
	}
}

func TestReadObject(t *testing.T) {
	// Setup test
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)

	// Given
	client, err := New(context.Background())
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	defer client.Close()

	expectedResult := []byte("#This text will be removed# *** START OF THIS PROJECT GUTENBERG EBOOK *** The " +
		"quick brown fox jumps over the lazy dog.")

	// When
	data, err := client.ReadObject(context.Background(), test.INPUT_BUCKET_NAME, "test.txt")

	// Then
	assert.Nil(t, err)
	assert.Equal(t, expectedResult, data)
}

func TestReadObject_CreateReaderError(t *testing.T) {
	// Setup test
	teardownStorage := test.CreateTestStorage(t)
	defer teardownStorage(t)

	// Given
	client, err := New(context.Background())
	if err != nil {
		t.Fatalf("Error creating storage client: %v", err)
	}
	defer client.Close()

	// When
	data, err := client.ReadObject(context.Background(), test.INPUT_BUCKET_NAME, "invalid-file.txt")

	// Then
	assert.NotNil(t, err)
	assert.Nil(t, data)
}
