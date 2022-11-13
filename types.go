package serverless_mapreduce

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type WordData struct {
	SortedWord string
	Word       string
}

type MapperData struct {
	Text []string `json:"text"`
}
