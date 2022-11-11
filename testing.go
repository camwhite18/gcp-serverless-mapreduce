package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"context"
	"os"
	"testing"
)

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
