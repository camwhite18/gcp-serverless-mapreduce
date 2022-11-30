package pubsub

import "time"

const MAX_MESSAGE_SIZE_BYTES = 50000
const MAX_MESSAGE_COUNT = 100
const MAX_MESSAGE_DELAY = 50 * time.Millisecond

const CONTROLLER_TOPIC = "mapreduce-controller"
const SPLITTER_TOPIC = "mapreduce-splitter"
const MAPPER_TOPIC = "mapreduce-mapper"
const COMBINE_TOPIC = "mapreduce-combine"
const SHUFFLER_TOPIC = "mapreduce-shuffler"
const REDUCER_TOPIC = "mapreduce-reducer"

const STATUS_STARTED = "started"
const STATUS_FINISHED = "finished"

type MessagePublishedData struct {
	Message PubSubMessage `json:"message"`
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type ControllerMessage struct {
	Id     string `json:"id"`
	Status string `json:"status"`
}

type MapperData struct {
	SortedWord string              `json:"sortedWord"`
	Anagrams   map[string]struct{} `json:"anagrams"`
}

type SplitterData struct {
	BucketName string `json:"bucketName"`
	FileName   string `json:"fileName"`
}
