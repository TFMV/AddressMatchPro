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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v2"
)

type Config struct {
	DBCreds struct {
		Host      string `yaml:"host"`
		Port      string `yaml:"port"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Database  string `yaml:"database"`
		LoadTable string `yaml:"load_table"`
	} `yaml:"db_creds"`
}

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
		if binaryKey.Len() >= 10 { // Ensure the binary key is 10 characters long
			break
		}
	}

	// Ensure the binary key is exactly 10 characters long
	for binaryKey.Len() < 10 {
		binaryKey.WriteString("0")
	}

	return binaryKey.String()
}

// ProcessCustomerAddresses processes customer addresses and generates binary keys
func ProcessCustomerAddresses(pool *pgxpool.Pool, referenceEntities []string, numWorkers int, runID int) {
	// Query the customer_matching table with the specified run_id
	rows, err := pool.Query(context.Background(), "SELECT customer_id, street FROM customer_matching WHERE run_id = $1", runID)
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
				log.Printf("Inserting batch of size %d into customer_keys\n", len(batch))
				InsertBatch(pool, batch, runID)
				batch = batch[:0] // reset batch
			}
		}
		if len(batch) > 0 {
			log.Printf("Inserting final batch of size %d into customer_keys\n", len(batch))
			InsertBatch(pool, batch, runID)
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

// InsertBatch inserts a batch of results into the database
func InsertBatch(pool *pgxpool.Pool, batch [][2]interface{}, runID int) {
	batchSize := len(batch)
	ids := make([]interface{}, batchSize)
	keys := make([]interface{}, batchSize)

	for i, record := range batch {
		ids[i] = record[0]
		keys[i] = record[1]
	}

	log.Printf("Executing batch insert with %d records\n", batchSize)
	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_keys (customer_id, binary_key, run_id) SELECT UNNEST($1::int[]), UNNEST($2::text[]), $3",
		ids, keys, runID,
	)
	if err != nil {
		log.Fatalf("Batch insert failed: %v\n", err)
	}
}

// ProcessSingleRecord processes a single record and inserts it into the database
func ProcessSingleRecord(pool *pgxpool.Pool, req MatchRequest) error {
	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_matching (first_name, last_name, phone_number, street, city, state, zip_code, run_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		strings.ToLower(req.FirstName), strings.ToLower(req.LastName), strings.ToLower(req.PhoneNumber),
		strings.ToLower(req.Street), strings.ToLower(req.City), strings.ToLower(req.State), strings.ToLower(req.ZipCode), req.RunID)

	if err != nil {
		log.Printf("Failed to insert single record: %v\n", err)
		return err
	}

	return nil
}

// Join converts a slice of floats to a comma-separated string
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

func CreateNewRun(pool *pgxpool.Pool, description string) int {
	var runID int
	err := pool.QueryRow(context.Background(),
		"INSERT INTO runs (description) VALUES ($1) RETURNING run_id",
		description,
	).Scan(&runID)
	if err != nil {
		log.Fatalf("Failed to create new run: %v\n", err)
	}
	return runID
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal config file: %v", err)
	}

	return &config, nil
}

func ClearOldCandidates(pool *pgxpool.Pool, runID int) {
	tables := []string{
		"customer_keys",
		"customer_tokens",
		"customer_vector_embedding",
	}
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s WHERE run_id = $1", table)
		if _, err := pool.Exec(context.Background(), query, runID); err != nil {
			fmt.Printf("Failed to clear old candidates from %s: %v\n", table, err)
		}
	}
}

// GenerateEmbeddingsPythonScript runs the Python script to generate embeddings.
func GenerateEmbeddingsPythonScript(scriptPath string, runID int) error {
	// Ensure the script path is absolute
	absScriptPath, err := filepath.Abs(scriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for script: %v", err)
	}

	// Set the working directory to the script's directory
	scriptDir := filepath.Dir(absScriptPath)

	cmd := exec.Command("python3", absScriptPath, strconv.Itoa(runID))
	cmd.Dir = scriptDir

	// Log the absolute script path and current working directory
	log.Printf("Running Python script: %s", absScriptPath)
	log.Printf("Set working directory to: %s", scriptDir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error generating embeddings: error running Python script: %v, output: %s", err, string(output))
		return fmt.Errorf("error running Python script: %v, output: %s", err, string(output))
	}
	return nil
}

// InsertFromLoadTable inserts records from the load_table into customer_matching
func InsertFromLoadTable(pool *pgxpool.Pool, runID int) error {
	_, err := pool.Exec(context.Background(),
		`INSERT INTO customer_matching (customer_id, first_name, last_name, phone_number, street, city, state, zip_code, run_id)
		 SELECT customer_id, LOWER(first_name), LOWER(last_name), phone_number, street, LOWER(city), LOWER(state), LOWER(zip_code::TEXT), $1 AS run_id
		 FROM batch_match`, runID)
	return err
}

// TruncateBatchMatchTable truncates the batch_match table
func TruncateBatchMatchTable(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), "TRUNCATE TABLE batch_match")
	return err
}

