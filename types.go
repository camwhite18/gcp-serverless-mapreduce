package serverless_mapreduce

import "time"

const MAX_MESSAGE_SIZE_BYTES = 1000000
const MAX_MESSAGE_COUNT = 10000
const MAX_MESSAGE_DELAY = 100 * time.Millisecond

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type ShufflerData struct {
	//Data []WordData `json:"data"`
}

type WordData struct {
	SortedWord string
	Word       string
}

type MapperData struct {
	Text []string `json:"text"`
}

type SplitterData struct {
	BucketName string   `json:"bucketName"`
	FileNames  []string `json:"fileNames"`
}
