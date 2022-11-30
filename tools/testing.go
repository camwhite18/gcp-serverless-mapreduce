package tools

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/iterator"
	"os"
	"strings"
	"testing"
	"time"
)

const INPUT_BUCKET_NAME = "test-bucket-input"
const OUTPUT_BUCKET_NAME = "test-bucket-output"

func SetupTest(tb testing.TB, topicIDs []string) (func(tb testing.TB), []*pubsub.Subscription) {
	// Setup test
	// Modify the PUBSUB_EMULATOR_HOST environment variable to point to the pubsub emulator
	existingVal := os.Getenv("PUBSUB_EMULATOR_HOST")
	err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a pubsub client so we can create a topic and subscription
	ctx, cancel := context.WithTimeout(context.Background(), 10)
	defer cancel()
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		tb.Fatalf("Error creating pubsub client: %v", err)
	}
	topics := make([]*pubsub.Topic, len(topicIDs))
	subscriptions := make([]*pubsub.Subscription, len(topicIDs))
	for i, topicID := range topicIDs {
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
		topics[i] = t
		subscriptions[i] = subscription
	}

	return func(tb testing.TB) {
		// Teardown test
		// Delete the subscriptions
		for i := 0; i < len(topicIDs); i++ {
			if err := subscriptions[i].Delete(context.Background()); err != nil {
				tb.Fatalf("Error deleting subscription: %v", err)
			}
			// Delete the topics
			if err := topics[i].Delete(context.Background()); err != nil {
				tb.Fatalf("Error deleting topic: %v", err)
			}
		}
		// Reset the PUBSUB_EMULATOR_HOST environment variable
		err = os.Setenv("PUBSUB_EMULATOR_HOST", existingVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}, subscriptions
}

func CreateTestStorage(tb testing.TB) func(tb testing.TB) {
	// Setup test
	ctx := context.Background()
	// Modify the STORAGE_EMULATOR_HOST environment variable to point to the storage emulator
	existingStorageVal := os.Getenv("STORAGE_EMULATOR_HOST")
	err := os.Setenv("STORAGE_EMULATOR_HOST", "localhost:9023")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a storage client so we can create buckets
	client, err := storage.NewClient(ctx)
	if err != nil {
		tb.Fatalf("Error creating storage client: %v", err)
	}
	// Create the input bucket
	inputBucket := client.Bucket(INPUT_BUCKET_NAME)
	createStorageCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := inputBucket.Create(createStorageCtx, "serverless-mapreduce", nil); err != nil &&
		!strings.Contains(err.Error(), "already own this bucket") {
		tb.Fatalf("Error creating inputBucket: %v", err)
	}
	// Create a file in the bucket
	object := inputBucket.Object("test.txt")
	writer := object.NewWriter(createStorageCtx)
	if _, err := writer.Write([]byte("#This text will be removed# *** START OF THIS PROJECT GUTENBERG EBOOK *** The " +
		"quick brown fox jumps over the lazy dog.")); err != nil {
		tb.Fatalf("Error writing to inputBucket: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("Error closing inputBucket: %v", err)
	}
	// Create the output bucket
	outputBucket := client.Bucket(OUTPUT_BUCKET_NAME)
	createOutputStorageCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := outputBucket.Create(createOutputStorageCtx, "serverless-mapreduce", nil); err != nil &&
		!strings.Contains(err.Error(), "already own this bucket") {
		tb.Fatalf("Error creating inputBucket: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		deleteStorageCtx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		// Delete the file in the bucket
		if err := object.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting object: %v", err)
		}
		// Delete the input bucket
		if err := inputBucket.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting input bucket: %v", err)
		}
		// Delete the files in the output bucket
		it := outputBucket.Objects(deleteStorageCtx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				tb.Fatalf("Error listing output bucket: %v", err)
			}
			if err := outputBucket.Object(attrs.Name).Delete(deleteStorageCtx); err != nil {
				tb.Fatalf("Error deleting object: %v", err)
			}
		}
		// Delete the output bucket
		if err := outputBucket.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting output bucket: %v", err)
		}
		// Reset the STORAGE_EMULATOR_HOST environment variable
		err = os.Setenv("STORAGE_EMULATOR_HOST", existingStorageVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}
}

func SetupRedisTest(tb testing.TB) func(tb testing.TB) {
	// Setup test
	existingRedisHostsVal := os.Getenv("REDIS_HOSTS")
	err := os.Setenv("REDIS_HOSTS", "localhost localhost localhost localhost localhost")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		//conn := RedisPool.Get()
		//defer conn.Close()
		//_, err := conn.Do("FLUSHALL")
		//if err != nil {
		//	tb.Fatalf("Error getting data from redis: %v", err)
		//}
		// Reset the REDIS_HOST environment variable
		err = os.Setenv("REDIS_HOSTS", existingRedisHostsVal)
	}
}
