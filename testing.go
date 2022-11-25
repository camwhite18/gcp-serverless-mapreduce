package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"strings"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10)
	defer cancel()
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
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
	if err := bucket.Create(createStorageCtx, "serverless-mapreduce", nil); err != nil &&
		!strings.Contains(err.Error(), "already own this bucket") {
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
