package serverlessmapreduce

import (
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"gitlab.com/cameron_w20/serverless-mapreduce/controller"
	"gitlab.com/cameron_w20/serverless-mapreduce/mapphase"
	"gitlab.com/cameron_w20/serverless-mapreduce/reducephase"
)

func init() {
	// Register all the functions
	functions.HTTP("Starter", mapphase.StartMapReduce)
	functions.CloudEvent("Controller", controller.Controller)
	functions.CloudEvent("Splitter", mapphase.Splitter)
	functions.CloudEvent("Mapper", mapphase.Mapper)
	functions.CloudEvent("Combiner", mapphase.Combine)
	functions.CloudEvent("Shuffler", reducephase.Shuffler)
	functions.CloudEvent("Reducer", reducephase.Reducer)
}
