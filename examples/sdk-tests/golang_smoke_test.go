//go:build ignore
// +build ignore

/*
Miroir SDK smoke test — Go
Tests: create index, add documents, search, update settings, delete index

Requirements:
  go get github.com/meilisearch/meilisearch-go

Run against docker-compose-dev:
  MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key go run golang_smoke_test.go
*/

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

type Document struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Author string `json:"author"`
	Year   int    `json:"year"`
}

func main() {
	url := os.Getenv("MIROIR_URL")
	if url == "" {
		url = "http://localhost:7700"
	}
	apiKey := os.Getenv("MIROIR_MASTER_KEY")
	if apiKey == "" {
		apiKey = "dev-key"
	}

	fmt.Println("=== Miroir Go SDK Smoke Test ===")
	fmt.Printf("Target: %s\n", url)

	client := meilisearch.NewClient(meilisearch.ClientConfig{
		Host:   url,
		APIKey: apiKey,
	})

	indexName := "test_golang_sdk"

	// Clean up any existing test index
	client.DeleteIndex(indexName)
	fmt.Printf("✓ Cleaned up existing index '%s'\n", indexName)

	// 1. Create index
	fmt.Println("\n1. Creating index...")
	index := client.Index(indexName)
	task, err := index.CreateIndex(&meilisearch.IndexConfig{
		Uid:        indexName,
		PrimaryKey: "id",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("   ✓ Created index '%s' with primary key 'id' (task %d)\n", indexName, task.TaskUID)

	time.Sleep(500 * time.Millisecond)

	// 2. Add documents
	fmt.Println("\n2. Adding documents...")
	documents := []Document{
		{ID: 1, Title: "The Great Gatsby", Author: "F. Scott Fitzgerald", Year: 1925},
		{ID: 2, Title: "To Kill a Mockingbird", Author: "Harper Lee", Year: 1960},
		{ID: 3, Title: "1984", Author: "George Orwell", Year: 1949},
	}
	task, err = index.AddDocuments(documents)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   ✓ Added %d documents (task %d)\n", len(documents), task.TaskUID)

	time.Sleep(1 * time.Second)

	// 3. Search
	fmt.Println("\n3. Searching...")
	searchRes, err := index.Search("gatsby", &meilisearch.SearchRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("   ✓ Found %d hits for 'gatsby'\n", len(searchRes.Hits))

	if len(searchRes.Hits) != 1 {
		panic(fmt.Sprintf("Expected 1 hit, got %d", len(searchRes.Hits)))
	}

	hit := searchRes.Hits[0].(map[string]interface{})
	if hit["title"] != "The Great Gatsby" {
		panic(fmt.Sprintf("Expected 'The Great Gatsby', got '%v'", hit["title"]))
	}

	// 4. Update settings
	fmt.Println("\n4. Updating settings...")
	settings := meilisearch.Settings{
		SearchableAttributes: []string{"title", "author"},
		FilterableAttributes: []string{"year"},
	}
	task, err = index.UpdateSettings(&settings)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   ✓ Updated settings (task %d)\n", task.TaskUID)

	time.Sleep(1 * time.Second)

	// 5. Delete index
	fmt.Println("\n5. Deleting index...")
	task, err = client.DeleteIndex(indexName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   ✓ Deleted index '%s' (task %d)\n", indexName, task.TaskUID)

	fmt.Println("\n=== All Go SDK tests passed! ===")
}
