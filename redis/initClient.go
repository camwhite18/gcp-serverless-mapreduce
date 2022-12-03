package redis

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"strconv"
	"strings"
)

// NoOfRedisInstances is the number of redis instances that sit in between the shufflers and the reducers
const NoOfRedisInstances = 5

// SingleRedisClient is a redis client that can be used to hold a single redis client.
var SingleRedisClient *redis.Client

// MultiRedisClient is a redis client that can be used to hold a map of redis clients.
var MultiRedisClient map[string]*redis.Client

// InitSingleRedisClient initializes a single redis client for the REDIS_HOST environment variable. This environment
// variable should be a single redis host address e.g. "10.1.1.1".
func InitSingleRedisClient() {
	if SingleRedisClient != nil {
		return
	}
	redisAddress := fmt.Sprintf("%s:6379", os.Getenv("REDIS_HOST"))
	SingleRedisClient = createClient(redisAddress)
}

// InitMultiRedisClient initializes a map of redis clients for the REDIS_HOSTS environment variable. This environment
// variable should be a space separated string of redis host addresses e.g. "10.1.1.1 10.2.2.2 10.3.3.3 10.4.4.4 10.5.5.5".
func InitMultiRedisClient() {
	if MultiRedisClient != nil {
		return
	}
	redisHosts := os.Getenv("REDIS_HOSTS")
	MultiRedisClient = make(map[string]*redis.Client)
	// Create a redis pool and return it
	for i, host := range strings.Split(redisHosts, " ") {
		MultiRedisClient[strconv.Itoa(i)] = createClient(fmt.Sprintf("%s:6379", host))
	}
}

// createClient creates a redis client for the given address
func createClient(redisAddress string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
		PoolSize: 100,
	})
}
