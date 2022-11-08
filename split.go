package serverless_mapreduce

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
)

func init() {
	functions.CloudEvent("Splitter", splitter)
}

func splitter(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("error getting data from event: %v", err)
	}
	log.Printf("Splitter received message: %v", string(msg.Message.Data))
	return nil
}
