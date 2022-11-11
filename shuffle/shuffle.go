package shuffle

import (
	"encoding/json"
	"gitlab.com/cameron_w20/serverless-mapreduce"
	"gitlab.com/cameron_w20/serverless-mapreduce/map"
	"io"
	"log"
	"net/http"
)

var anagramMap = make(map[string][]string)

func shuffle(w http.ResponseWriter, r *http.Request) {
	var msg serverless_mapreduce.MessagePublishedData
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
	// Unmarshal the message data
	wordData := _map.WordData{}
	if err := json.Unmarshal(msg.Message.Data, &wordData); err != nil {
		log.Printf("Error unmarshalling message data: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	anagramMap[wordData.SortedWord] = append(anagramMap[wordData.SortedWord], wordData.Word)
	log.Printf("Anagram map: %v", anagramMap)
}

func main() {
	http.HandleFunc("/", shuffle)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
