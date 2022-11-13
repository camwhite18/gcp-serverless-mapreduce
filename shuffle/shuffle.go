package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

var anagramMap = make(map[string]map[string][]string)

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
	id := msg.Message.Attributes["instanceId"]
	// Unmarshal the message data
	wordData := WordData{}
	if err := json.Unmarshal(msg.Message.Data, &wordData); err != nil {
		log.Printf("Error unmarshalling message data: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	anagramMap[id][wordData.SortedWord] = append(anagramMap[id][wordData.SortedWord], wordData.Word)
	log.Printf("Anagram map: %v", anagramMap)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", shuffle)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
