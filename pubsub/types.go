package pubsub

import "time"

const MaxMessageSizeBytes = 50000
const MaxMessageCount = 100
const MaxMessageDelay = 50 * time.Millisecond

const ControllerTopic = "mapreduce-controller"
const SplitterTopic = "mapreduce-splitter"
const MapperTopic = "mapreduce-mapper"
const CombineTopic = "mapreduce-combine"
const ShufflerTopic = "mapreduce-shuffler"
const ReducerTopic = "mapreduce-reducer"

const StatusStarted = "started"
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
