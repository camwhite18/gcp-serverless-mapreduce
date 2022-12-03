package pubsub

import "time"

// MaxMessageSizeBytes is the maximum size of a pubsub message in bytes.
const MaxMessageSizeBytes = 50000

// MaxMessageCount is the maximum number of messages that can be sent in a single batch.
const MaxMessageCount = 100

// MaxMessageDelay is the maximum delay between sending messages in a batch.
const MaxMessageDelay = 50 * time.Millisecond

// ControllerTopic is the name of the topic that the controller reads from.
const ControllerTopic = "mapreduce-controller"

// SplitterTopic is the name of the topic that the splitter reads from.
const SplitterTopic = "mapreduce-splitter"

// MapperTopic is the name of the topic that the mapper reads from.
const MapperTopic = "mapreduce-mapper"

// CombineTopic is the name of the topic that the reducer reads from.
const CombineTopic = "mapreduce-combiner"

// ShufflerTopic is the name of the topic that the shuffler reads from.
const ShufflerTopic = "mapreduce-shuffler"

// ReducerTopic is the name of the topic that the reducer reads from.
const ReducerTopic = "mapreduce-reducer"

// StatusStarted is the status of a partition when it has been pushed to the MapperTopic.
const StatusStarted = "started"

// StatusFinished is the status of a partition when its mapped text has been added to the Redis instances.
const StatusFinished = "finished"

// MessagePublishedData is a struct that represents the data of a pubsub message published event.
type MessagePublishedData struct {
	Message Message `json:"message"`
}

// Message is a JSON field of MessagePublishedData.
type Message struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// ControllerMessage is a message sent to the controller.
type ControllerMessage struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// MappedWord is the output of the mapper.
type MappedWord struct {
	SortedWord string              `json:"sortedWord"`
	Anagrams   map[string]struct{} `json:"anagrams"`
}

// SplitterData is the data sent to the splitter.
type SplitterData struct {
	BucketName string `json:"bucketName"`
	FileName   string `json:"fileName"`
}
