package serverless_mapreduce

import "time"

const NO_OF_REDUCER_INSTANCES = 5

const MAX_MESSAGE_SIZE_BYTES = 100000
const MAX_MESSAGE_COUNT = 100
const MAX_MESSAGE_DELAY = 50 * time.Millisecond

const STATUS_STARTED = "started"
const STATUS_FINISHED = "finished"

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type StatusMessage struct {
	Id         string
	Status     string
	ReducerNum string
}

type WordData struct {
	SortedWord string
	Anagrams   map[string]struct{}
}

type SplitterData struct {
	BucketName string `json:"bucketName"`
	FileName   string `json:"fileName"`
}
