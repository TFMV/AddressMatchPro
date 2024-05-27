// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice shall be included in all copies or substantial
// portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package matcher

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Load reference entities into memory
func LoadReferenceEntities(pool *pgxpool.Pool) []string {
	rows, err := pool.Query(context.Background(), "SELECT entity_value FROM reference_entities")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	var referenceEntities []string
	for rows.Next() {
		var entityValue string
		err := rows.Scan(&entityValue)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		referenceEntities = append(referenceEntities, entityValue)
	}
	return referenceEntities
}

// Calculate the binary key for a given street address
func CalculateBinaryKey(referenceEntities []string, street string) string {
	var binaryKey strings.Builder
	n := 2 // Adjust the n-gram size to capture more variations

	for _, referenceStreet := range referenceEntities {
		similarity := ngramFrequencySimilarity(street, referenceStreet, n)
		if similarity >= 0.1 { // Further reduce the threshold to be less restrictive
			binaryKey.WriteString("1")
		} else {
			binaryKey.WriteString("0")
		}
		if binaryKey.Len() >= 10 { // Ensure the binary key is 20 characters long
			break
		}
	}

	// Ensure the binary key is exactly 20 characters long
	for binaryKey.Len() < 10 {
		binaryKey.WriteString("0")
	}

	return binaryKey.String()
}

// Insert a batch of results into the database
func InsertBatch(pool *pgxpool.Pool, batch [][2]interface{}, run_id int) {
	batchSize := len(batch)
	ids := make([]interface{}, batchSize)
	keys := make([]interface{}, batchSize)

	for i, record := range batch {
		ids[i] = record[0]
		keys[i] = record[1]
	}

	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_keys (customer_id, binary_key, run_id) SELECT UNNEST($1::int[]), UNNEST($2::text[]), $3",
		ids, keys, run_id,
	)
	if err != nil {
		log.Fatalf("Batch insert failed: %v\n", err)
	}
}

// Process customer addresses and generate binary keys
func ProcessCustomerAddresses(pool *pgxpool.Pool, referenceEntities []string, numWorkers int, run_id int) {
	rows, err := pool.Query(context.Background(), "SELECT customer_id as id, customer_street as street FROM customers")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	var wg sync.WaitGroup
	addressCh := make(chan [2]interface{}, 1000)
	resultCh := make(chan [2]interface{}, 1000)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addressCh {
				id := addr[0].(int)
				street := addr[1].(string)

				standardizedStreet, err := StandardizeAddress(street)
				if err != nil {
					log.Printf("Failed to standardize address: %v\n", err)
					continue
				}
				binaryKey := CalculateBinaryKey(referenceEntities, strings.ToLower(standardizedStreet))
				resultCh <- [2]interface{}{id, binaryKey}
			}
		}()
	}

	// Insert results in batches
	go func() {
		var batchSize = 1000
		var batch [][2]interface{}
		for res := range resultCh {
			batch = append(batch, res)
			if len(batch) >= batchSize {
				InsertBatch(pool, batch, run_id)
				batch = batch[:0] // reset batch
			}
		}
		if len(batch) > 0 {
			InsertBatch(pool, batch, run_id)
		}
	}()

	// Enqueue addresses for processing
	for rows.Next() {
		var id int
		var street string
		err := rows.Scan(&id, &street)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		addressCh <- [2]interface{}{id, street}
	}
	close(addressCh)
	wg.Wait()
	close(resultCh)
}

// ProcessSingleRecord processes a single record and inserts it into the database
func ProcessSingleRecord(pool *pgxpool.Pool, req MatchRequest) {
	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_matching (first_name, last_name, phone_number, street, city, state, zip_code, run_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		req.FirstName, req.LastName, req.PhoneNumber, req.Street, req.City, req.State, req.ZipCode, req.RunID)
	if err != nil {
		fmt.Printf("Failed to process single record: %v\n", err)
	}
}

// generateEmbeddingsPythonScript runs the Python script to generate embeddings for a given run ID
func generateEmbeddingsPythonScript(scriptPath string, runID int) error {
	cmd := exec.Command("python3", scriptPath, fmt.Sprintf("--run_id=%d", runID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error running Python script: %v, output: %s", err, string(output))
	}
	return nil
}

// join converts a slice of floats to a comma-separated string
func join(slice []float64, sep string) string {
	str := ""
	for i, v := range slice {
		if i > 0 {
			str += sep
		}
		str += fmt.Sprintf("%f", v)
	}
	return str
}

// CreateNewRun creates a new run entry in the database and returns the run ID
func CreateNewRun(pool *pgxpool.Pool, description string) int {
	var runID int
	err := pool.QueryRow(context.Background(), "INSERT INTO runs (description) VALUES ($1) RETURNING run_id", description).Scan(&runID)
	if err != nil {
		log.Fatalf("Failed to create new run: %v\n", err)
	}
	return runID
}
