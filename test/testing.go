package test

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"gitlab.com/cameron_w20/serverless-mapreduce/redis"
	"google.golang.org/api/iterator"
	"os"
	"strings"
	"testing"
	"time"
)

// InputBucketName is the name of the bucket that contains the input files in testing
const InputBucketName = "test-bucket-input"

// OutputBucketName is the name of the bucket that contains the output files in testing
const OutputBucketName = "test-bucket-output"

// SetupPubSubTest creates subscriptions for testing pubsub messages. It points the PUBSUB_EMULATOR_HOST environment
// variable towards localhost:8085 that one could run this Docker image to emulate pubsub:
// gcr.io/google.com/cloudsdktool/cloud-sdk:latest
func SetupPubSubTest(tb testing.TB, topicIDs []string) (func(tb testing.TB), []*pubsub.Subscription) {
	// Setup test
	existingGCPProject := os.Getenv("GCP_PROJECT")
	err := os.Setenv("GCP_PROJECT", "serverless-mapreduce")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Modify the PUBSUB_EMULATOR_HOST environment variable to point to the pubsub emulator
	existingVal := os.Getenv("PUBSUB_EMULATOR_HOST")
	err = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
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
		// Reset the GCP_PROJECT environment variable
		err = os.Setenv("GCP_PROJECT", existingGCPProject)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}, subscriptions
}

// SetupStorageTest creates buckets and an object for testing. It points the STORAGE_EMULATOR_HOST environment variable
// towards localhost:9023 that one could run this Docker image to emulate pubsub: oittaa/gcp-storage-emulator
func SetupStorageTest(tb testing.TB) func(tb testing.TB) {
	// Setup test
	createStorageCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	// Modify the STORAGE_EMULATOR_HOST environment variable to point to the storage emulator
	existingStorageVal := os.Getenv("STORAGE_EMULATOR_HOST")
	err := os.Setenv("STORAGE_EMULATOR_HOST", "localhost:9023")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Create a storage client so we can create buckets
	client, err := storage.NewClient(createStorageCtx)
	if err != nil {
		tb.Fatalf("Error creating storage client: %v", err)
	}
	// Create the input bucket
	inputBucket := client.Bucket(InputBucketName)
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
	outputBucket := client.Bucket(OutputBucketName)
	defer cancel()
	if err := outputBucket.Create(createStorageCtx, "serverless-mapreduce", nil); err != nil &&
		!strings.Contains(err.Error(), "already own this bucket") {
		tb.Fatalf("Error creating outputBucket: %v", err)
	}

	return func(tb testing.TB) {
		// Teardown test
		deleteStorageCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		// Delete the files in the input bucket
		it := inputBucket.Objects(deleteStorageCtx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				tb.Fatalf("Error listing output bucket: %v", err)
			}
			_ = inputBucket.Object(attrs.Name).Delete(deleteStorageCtx)
		}
		// Delete the input bucket
		if err := inputBucket.Delete(deleteStorageCtx); err != nil {
			tb.Fatalf("Error deleting input bucket: %v", err)
		}
		// Delete the files in the output bucket
		it = outputBucket.Objects(deleteStorageCtx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				tb.Fatalf("Error listing output bucket: %v", err)
			}
			_ = outputBucket.Object(attrs.Name).Delete(deleteStorageCtx)
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

// SetupRedisTest creates a Redis client and points the REDIS_HOST/REDIS_HOSTS environment variables towards
// localhost that one could run this Docker image to emulate Redis: redis/redis-stack:latest
func SetupRedisTest(tb testing.TB) func(tb testing.TB) {
	// Replace the REDIS_HOST environment variable with localhost
	existingRedisHostVal := os.Getenv("REDIS_HOST")
	err := os.Setenv("REDIS_HOST", "localhost")
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Replace the REDIS_HOSTS environment variable with localhost
	existingRedisHostsVal := os.Getenv("REDIS_HOSTS")
	var redisHosts []string
	for i := 0; i < redis.NoOfReducerJobs; i++ {
		redisHosts = append(redisHosts, "localhost")
	}
	err = os.Setenv("REDIS_HOSTS", strings.Join(redisHosts, " "))
	if err != nil {
		tb.Fatalf("Error setting environment variable: %v", err)
	}
	// Initialize the Redis clients
	redis.InitSingleRedisClient()
	redis.InitMultiRedisClient()

	return func(tb testing.TB) {
		// Teardown test
		redis.SingleRedisClient.FlushAll(context.Background())
		// Reset the REDIS_HOSTS environment variable
		err = os.Setenv("REDIS_HOSTS", existingRedisHostsVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
		// Reset the REDIS_HOST environment variable
		err = os.Setenv("REDIS_HOST", existingRedisHostVal)
		if err != nil {
			tb.Fatalf("Error setting environment variable: %v", err)
		}
	}
}
