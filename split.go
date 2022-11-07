package serverless_mapreduce

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"io"
	"log"
	"time"
)

func init() {
	functions.CloudEvent("Splitter", splitter)
}

type StorageObjectData struct {
	Bucket         string    `json:"bucket,omitempty"`
	Name           string    `json:"name,omitempty"`
	Metageneration int64     `json:"metageneration,string,omitempty"`
	TimeCreated    time.Time `json:"timeCreated,omitempty"`
	Updated        time.Time `json:"updated,omitempty"`
}

func splitter(ctx context.Context, e event.Event) error {
	var data StorageObjectData
	if err := e.DataAs(&data); err != nil {
		return err
	}
	text, err := readFileFromBucket(ctx, data.Bucket, data.Name)
	if err != nil {
		return err
	}
	client, err := pubsub.NewClient(ctx, "serverless-mapreduce")
	if err != nil {
		return err
	}
	defer client.Close()
	topic := client.Topic("mapreduce-mapper")
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        text,
		PublishTime: time.Now(),
	})
	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	log.Printf("Published a message; msg ID: %v", id)
	return nil
}

func readFileFromBucket(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	rc, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}
