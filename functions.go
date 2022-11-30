package serverless_mapreduce

import (
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"gitlab.com/cameron_w20/serverless-mapreduce/controller"
	"gitlab.com/cameron_w20/serverless-mapreduce/map_phase"
	"gitlab.com/cameron_w20/serverless-mapreduce/reduce_phase"
	"gitlab.com/cameron_w20/serverless-mapreduce/shuffle_phase"
)

func init() {
	// Register all the functions
	functions.HTTP("Starter", map_phase.StartMapReduce)
	functions.CloudEvent("Controller", controller.Controller)
	functions.CloudEvent("Splitter", map_phase.Splitter)
	functions.CloudEvent("Mapper", map_phase.Mapper)
	functions.CloudEvent("Combiner", shuffle_phase.Combine)
	functions.CloudEvent("Shuffler", shuffle_phase.Shuffler)
	functions.CloudEvent("Reducer", reduce_phase.Reducer)
}
