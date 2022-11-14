package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

type Maps struct {
	mu              sync.Mutex
	anagramMap      map[string]map[string][]string
	finishedMappers map[string]map[string]bool
}

var anagramMap = make(map[string]map[string][]string)

//var mapperFinishedMap = make(map[string]map[string]bool)

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

func (m *Maps) shuffle(w http.ResponseWriter, r *http.Request) {
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
	id := msg.Message.Attributes["instanceId"]
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := anagramMap[id]; !ok {
		anagramMap[id] = make(map[string][]string)
	}
	anagramMap[id][wordData.SortedWord] = append(anagramMap[id][wordData.SortedWord], wordData.Word)
	log.Printf("Anagram map: %v", anagramMap)
}

func main() {
	m := Maps{}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", m.shuffle)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
