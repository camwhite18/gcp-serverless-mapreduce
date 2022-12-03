package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"strings"
)

// Client is an interface for interacting with storage.
type Client interface {
	Close()
	ReadObjectNames(ctx context.Context, bucketName string) ([]string, error)
	ReadObject(ctx context.Context, bucketName, objectName string) ([]byte, error)
	WriteData(key string, value []string)
}

type clientImpl struct {
	client *storage.Client
	writer *storage.Writer
}

var _ Client = &clientImpl{}

// New returns a new storage client.
func New(ctx context.Context) (Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating storage client: %v", err)
	}
	return &clientImpl{
		client: client,
	}, nil
}

// NewWithWriter returns a new storage client with a writer for the given bucket and object.
func NewWithWriter(ctx context.Context, bucketName, objectName string) (Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating storage client: %v", err)
	}
	return &clientImpl{
		client: client,
		writer: client.Bucket(bucketName).Object(objectName).NewWriter(ctx),
	}, nil
}

// Close closes the storage client.
func (c *clientImpl) Close() {
	err := c.client.Close()
	if err != nil {
		log.Println("Error closing storage client: ", err)
	}
	// Close the writer if it exists
	if c.writer != nil {
		err = c.writer.Close()
		if err != nil {
			log.Println("Error closing storage writer: ", err)
		}
	}
}

// ReadObjectNames returns the names of all objects in the given bucket.
func (c *clientImpl) ReadObjectNames(ctx context.Context, bucketName string) ([]string, error) {
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
		// Add the file name to the list of files if it is a text file
		if strings.HasSuffix(attributes.Name, ".txt") {
			files = append(files, attributes.Name)
		}
	}
	return files, nil
}

// ReadObject returns the contents of the given object in the given bucket.
func (c *clientImpl) ReadObject(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	// Create a reader for the file
	rc, err := c.client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating reader for object %s: %v", objectName, err)
	}
	defer rc.Close()
	// Read the contents of the file
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WriteData writes the given data to the storage client's writer.
func (c *clientImpl) WriteData(key string, value []string) {
	// Create a string from the key and the value slice
	data := fmt.Sprintf("%s: %s\n", key, strings.Join(value, " "))
	// Write the data to the writer
	_, err := c.writer.Write([]byte(data))
	if err != nil {
		log.Println("Error writing data to file: ", err)
	}
}
