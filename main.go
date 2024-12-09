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

type Cache struct {
	Values map[string]any
	Mu     sync.Mutex
}

type Input struct {
	Value   any    `json:"value"`
	Key     string `json:"key"`
	Persist bool   `json:"persist"`
	Erase   bool   `json:"erase"`
}

type Response struct {
	Value   any
	Message string
	Key     string
}

type DB struct {
	File     *os.File
	Contents map[string]any // contents that persist on disk
	Mu       sync.Mutex
}

const PORT = 3000

var c = &Cache{
	Values: make(map[string]any),
}

var db = &DB{
	Contents: make(map[string]any),
}

func setupFile() {
	_, err := os.Stat("db.json")
	if os.IsNotExist(err) {
		_, err := os.Create("db.json")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func readFileContents() {
	f, err := os.OpenFile("db.json", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	db.File = f // db is a globally declared var
	err = json.NewDecoder(db.File).Decode(&db.Contents)
	if err != nil {
		if err == io.EOF { // file is empty
			return
		}
		log.Fatal(err)
	}
	for k, v := range db.Contents {
		c.Values[k] = v
	}
}

func deleteFromFile(i Input, e *error, wg *sync.WaitGroup) {
	defer wg.Done()
	db.Mu.Lock()
	defer db.Mu.Unlock()
	delete(db.Contents, i.Key)
	truncateAndEncode(db, e)
}

func truncateAndEncode(db *DB, e *error) {
	// Truncate the file
	err := db.File.Truncate(0) // Remove the old content
	if err != nil {
		*e = err
		return
	}
	encoder := json.NewEncoder(db.File)
	encoder.SetIndent("", "  ")       //  pretty print with indentation
	err = encoder.Encode(db.Contents) // Re-encode the map into the file
	if err != nil {
		*e = err
		return
	}
}

func appendToFile(i Input, e *error, wg *sync.WaitGroup) {
	defer wg.Done()
	db.Mu.Lock()
	defer db.Mu.Unlock()
	db.Contents[i.Key] = i.Value
	truncateAndEncode(db, e)
}

func updateValues(i Input, wg *sync.WaitGroup) {
	defer wg.Done()
	c.Mu.Lock()
	c.Values[i.Key] = i.Value
	c.Mu.Unlock()
}

func helloMsg(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello from GDB!")
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	var i Input
	err := json.NewDecoder(r.Body).Decode(&i)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go updateValues(i, &wg)

	var appendErr error = nil
	if i.Persist {
		wg.Add(1)
		go appendToFile(i, &appendErr, &wg)
	}

	wg.Wait()
	if appendErr != nil {
		fmt.Printf("Failed to persist data: %v \n", err)
		http.Error(w, "Failed to persist data to file", http.StatusInternalServerError)
		return
	}

	res := &Response{
		Message: "Added successfully",
		Key:     i.Key,
		Value:   i.Value,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(res)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	c.Mu.Lock()
	val, ok := c.Values[key]
	c.Mu.Unlock()
	if ok {
		res := &Response{
			Message: "Cache found",
			Key:     key,
			Value:   val,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(res)
		return
	}

	res := &Response{
		Message: "Cache not found",
		Key:     key,
		Value:   nil,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(res)
}

func deleteKey(i Input, wg *sync.WaitGroup) {
	defer wg.Done()
	c.Mu.Lock()
	delete(c.Values, i.Key)
	c.Mu.Unlock()
}

func handleRemove(w http.ResponseWriter, r *http.Request) {
	var i Input
	err := json.NewDecoder(r.Body).Decode(&i)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go deleteKey(i, &wg)

	var eraseErr error
	if i.Erase {
		wg.Add(1)
		go deleteFromFile(i, &eraseErr, &wg)
	}

	wg.Wait()
	if eraseErr != nil {
		fmt.Printf("Failed to delete data: %v \n", eraseErr)
		http.Error(w, "Failed to delete data", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	setupFile()
	readFileContents()
	defer db.File.Close()
	http.HandleFunc("/", helloMsg)
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/remove", handleRemove)

	fmt.Printf("Server running on: %d \n", PORT)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", PORT), nil))
}
