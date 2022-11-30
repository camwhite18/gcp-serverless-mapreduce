package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"strings"
	"time"
)

type Client interface {
	Close()
	ReadObjectNames(ctx context.Context, bucketName string) ([]string, error)
	ReadObject(ctx context.Context, bucketName, objectName string) ([]byte, error)
	CreateWriter(ctx context.Context, bucketName, objectName string)
	WriteData(key string, value []string)
}

type clientImpl struct {
	client *storage.Client
	writer *storage.Writer
}

var _ Client = &clientImpl{}

func New(ctx context.Context) (Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &clientImpl{
		client: client,
	}, nil
}

func (c *clientImpl) Close() {
	err := c.client.Close()
	if err != nil {
		log.Println("Error closing storage client: ", err)
	}
}

func (c *clientImpl) ReadObjectNames(ctx context.Context, bucketName string) ([]string, error) {
	// Create a 10-second timeout context so we don't wait forever if there is an error
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	// Iterate over all objects in the bucket and add each file name to the files slice
	objects := c.client.Bucket(bucketName).Objects(ctx, nil)
	files := make([]string, 0)
	for {
		attributes, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		// Add the file name to the list of files
		files = append(files, attributes.Name)
	}
	return files, nil
}

func (c *clientImpl) ReadObject(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	// Create a 10-second timeout context so we don't wait forever if there is an error
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	// Create a reader for the file
	rc, err := c.client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	// Read the contents of the file
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *clientImpl) CreateWriter(ctx context.Context, bucketName, objectName string) {
	// Create a 10-second timeout context so we don't wait forever if there is an error
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	// Create a writer for the file
	c.writer = c.client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
}

func (c *clientImpl) WriteData(key string, value []string) {
	data := fmt.Sprintf("%s: %s\n", key, strings.Join(value, " "))
	_, err := c.writer.Write([]byte(data))
	if err != nil {
		log.Println("Error writing data to file: ", err)
	}
}
