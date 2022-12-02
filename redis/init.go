package redis

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"strconv"
	"strings"
)

const NO_OF_REDIS_INSTANCES = 5

var RedisClient *redis.Client
var MultiRedisClient map[string]*redis.Client

func InitRedisClient() {
	if RedisClient != nil {
		return
	}
	redisAddress := fmt.Sprintf("%s:6379", os.Getenv("REDIS_HOST"))
	RedisClient = createClient(redisAddress)
}

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

func createClient(redisAddress string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
		PoolSize: 100,
	})
}
