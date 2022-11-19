package main

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"io"
	"log"
	"net/http"
	"os"
)

//var mu sync.Mutex
//var anagramMap = make(map[string]map[string][]string)

//var mapperFinishedMap = make(map[string]map[string]bool)

var redisPool *redis.Pool

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

func shuffle(w http.ResponseWriter, r *http.Request) {
	var msg MessagePublishedData
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	// Get the mapreduce instance id
	//id := msg.Message.Attributes["instanceId"]
	// Check for finished tag from all mappers
	// Then send the results to the reducer
	// Unmarshal the message data
	wordData := WordData{}
	if err := json.Unmarshal(msg.Message.Data, &wordData); err != nil {
		log.Printf("Error unmarshalling message data: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	// Lock the mutex to prevent concurrent access to the map
	//mu.Lock()
	//defer mu.Unlock()
	//if _, ok := anagramMap[id]; !ok {
	//	anagramMap[id] = make(map[string][]string)
	//}
	//anagramMap[id][wordData.SortedWord] = append(anagramMap[id][wordData.SortedWord], wordData.Word)
	//log.Printf("Anagram map: %v", anagramMap)
	conn := redisPool.Get()
	defer conn.Close()

	li, err := conn.Do("GET", wordData.SortedWord)
	if err != nil {
		log.Printf("Error getting data from redis: %v", err)
		return
	}
	var words []string
	if li == nil {
		words = []string{wordData.Word}
	} else {
		err := json.Unmarshal(li.([]byte), &words)
		if err != nil {
			log.Printf("Error unmarshalling data from redis: %v", err)
			return
		}
		words = append(words, wordData.Word)
	}
	liBytes, err := json.Marshal(words)
	if err != nil {
		log.Printf("Error marshalling data to redis: %v", err)
		return
	}
	_, err = conn.Do("SET", wordData.SortedWord, liBytes)
	if err != nil {
		log.Printf("Error setting data in redis: %v", err)
		return
	}
}

func main() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)
	const maxConnections = 10
	redisPool = &redis.Pool{
		MaxIdle: maxConnections,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddress)
		},
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", shuffle)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
