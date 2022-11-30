package tools

import (
	"github.com/go-redis/redis/v8"
	"time"
)

const NO_OF_REDUCER_INSTANCES = 5

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

var RedisPool *redis.Client
var ReducerRedisPool []*redis.Client

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type StatusMessage struct {
	Id     string
	Status string
}

type WordData struct {
	SortedWord string
	Anagrams   map[string]struct{}
}

type SplitterData struct {
	BucketName string `json:"bucketName"`
	FileName   string `json:"fileName"`
}
