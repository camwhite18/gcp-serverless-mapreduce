package serverless_mapreduce

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type ShufflerData struct {
	Data []WordData `json:"data"`
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
