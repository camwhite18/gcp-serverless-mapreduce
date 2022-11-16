package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"os"
	"testing"
	"time"
)

const BUCKET_NAME = "test-bucket"

func SetupTest(tb testing.TB, topicID string) (func(tb testing.TB), *pubsub.Subscription) {
	// Setup test
	// Modify the PUBSUB_EMULATOR_HOST environment variable to point to the pubsub emulator
	existingVal := os.Getenv("PUBSUB_EMULATOR_HOST")
	err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a pubsub client so we can create a topic and subscription
	client, err := pubsub.NewClient(context.Background(), "serverless-mapreduce")
	if err != nil {
		tb.Fatalf("Error creating pubsub client: %v", err)
	}
	t, err := client.CreateTopic(context.Background(), topicID)
	if err != nil {
		tb.Fatalf("Error creating topic: %v", err)
	}
	subscription, err := client.CreateSubscription(context.Background(), topicID, pubsub.SubscriptionConfig{
		Topic: t,
	})
	if err != nil {
		tb.Fatalf("Error creating subscription: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		// Delete the subscription
		if err := subscription.Delete(context.Background()); err != nil {
			tb.Fatalf("Error deleting subscription: %v", err)
		}
		// Delete the topic
		if err := t.Delete(context.Background()); err != nil {
			tb.Fatalf("Error deleting topic: %v", err)
		}
		// Reset the PUBSUB_EMULATOR_HOST environment variable
		err = os.Setenv("PUBSUB_EMULATOR_HOST", existingVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}, subscription
}

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
	createStorageCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := bucket.Create(createStorageCtx, "serverless-mapreduce", nil); err != nil {
		tb.Fatalf("Error creating bucket: %v", err)
	}
	// Create a file in the bucket
	object := bucket.Object("test.txt")
	writer := object.NewWriter(createStorageCtx)
	if _, err := writer.Write([]byte("#This text will be removed# *** START OF THIS PROJECT GUTENBERG EBOOK *** The " +
		"quick brown fox jumps over the lazy dog.")); err != nil {
		tb.Fatalf("Error writing to bucket: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("Error closing bucket: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		deleteStorageCtx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		// Delete the file in the bucket
		if err := object.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting object: %v", err)
		}
		// Delete the bucket
		if err := bucket.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting bucket: %v", err)
		}
		// Reset the STORAGE_EMULATOR_HOST environment variable
		err = os.Setenv("STORAGE_EMULATOR_HOST", existingStorageVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}
}
