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
// The above copyright notice shall be included in all
// copies or substantial portions of the Software.
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

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

func generateEmbeddingsPythonScript(scriptPath string, runID int) error {
	cmd := exec.Command("python3", scriptPath, strconv.Itoa(runID))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error running Python script: %v, output: %s", err, string(output))
	}
	return nil
}

func clearOldCandidates(pool *pgxpool.Pool) {
	tables := []string{
		"customer_keys",
		"customer_tokens",
		"tokens_idf",
		"customer_vector_embedding",
	}
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s WHERE run_id = 0", table)
		if _, err := pool.Exec(context.Background(), query); err != nil {
			log.Fatalf("Failed to clear old candidates from %s: %v", table, err)
		}
	}
}

func clearAndInsertDefaultRun(pool *pgxpool.Pool) {
	// Clear existing entry with run_id = 0 from runs table
	clearQuery := "DELETE FROM runs WHERE run_id = 0"
	if _, err := pool.Exec(context.Background(), clearQuery); err != nil {
		log.Fatalf("Failed to clear runs for run_id = 0: %v", err)
	}

	// Insert default run_id = 0 into runs table
	insertQuery := "INSERT INTO runs (run_id, description) VALUES (0, 'Default run')"
	if _, err := pool.Exec(context.Background(), insertQuery); err != nil {
		log.Fatalf("Failed to insert default run: %v", err)
	}
	fmt.Println("Default run inserted successfully")
}

func syncCustomerMatchingWithRun(pool *pgxpool.Pool) {
	// Clear existing entries in customer_matching with run_id = 0
	clearQuery := "DELETE FROM customer_matching WHERE run_id = 0"
	if _, err := pool.Exec(context.Background(), clearQuery); err != nil {
		log.Fatalf("Failed to clear customer_matching for run_id = 0: %v", err)
	}

	// Insert rows into customer_matching with run_id = 0
	insertQuery := `
		INSERT INTO customer_matching (customer_id, first_name, last_name, phone_number, street, city, state, zip_code, run_id)
		SELECT customer_id, LOWER(customer_fname), LOWER(customer_lname), NULL AS phone_number, LOWER(customer_street), LOWER(customer_city), LOWER(customer_state), LOWER(customer_zipcode::TEXT), 0 AS run_id
		FROM customers;
	`
	if _, err := pool.Exec(context.Background(), insertQuery); err != nil {
		log.Fatalf("Failed to insert into customer_matching for run_id = 0: %v", err)
	}
	fmt.Println("Customer matching table synced with run_id = 0")
}

func main() {
	start := time.Now()

	// Load the configuration
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/Users/thomasmcgeehan/FuzzyMatchFinder/FuzzyMatchFinder/config.yaml" // Default path for local development
	}

	config, err := matcher.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fmt.Println("Config loaded successfully")

	// Create the database connection string
	databaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		config.DBCreds.Username,
		config.DBCreds.Password,
		config.DBCreds.Host,
		config.DBCreds.Port,
		config.DBCreds.Database,
	)

	// Parse the database connection string
	dbConfig, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v\n", err)
	}

	// Create the connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer pool.Close()
	fmt.Println("Database connection pool created successfully")

	// Clear existing run_id = 0 and insert default run into runs table
	stepStart := time.Now()
	clearAndInsertDefaultRun(pool)
	fmt.Printf("Default run inserted in %v\n", time.Since(stepStart))

	// Clear old candidates with run_id = 0
	stepStart = time.Now()
	clearOldCandidates(pool)
	fmt.Printf("Old candidates cleared in %v\n", time.Since(stepStart))

	// Sync customer_matching table with run_id = 0
	stepStart = time.Now()
	syncCustomerMatchingWithRun(pool)
	fmt.Printf("Customer matching table synced in %v\n", time.Since(stepStart))

	// Load reference entities once
	stepStart = time.Now()
	referenceEntities := matcher.LoadReferenceEntities(pool)
	fmt.Printf("Reference entities loaded in %v\n", time.Since(stepStart))

	// Process customer addresses and generate binary keys with concurrency
	stepStart = time.Now()
	matcher.ProcessCustomerAddresses(pool, referenceEntities, 10, 0) // Passing run_id = 0
	fmt.Printf("Customer addresses processed in %v\n", time.Since(stepStart))

	// Generate TF/IDF vectors
	stepStart = time.Now()
	matcher.GenerateTFIDF(pool, 0) // Passing run_id = 0
	fmt.Printf("TF/IDF vectors generated in %v\n", time.Since(stepStart))

	// Insert vector embeddings using Python script
	stepStart = time.Now()
	scriptPath := os.Getenv("SCRIPT_PATH")
	if scriptPath == "" {
		scriptPath = "./python-ml/generate_embeddings.py" // Default path for local development
	}
	if err := generateEmbeddingsPythonScript(scriptPath, 0); err != nil {
		log.Fatalf("Failed to generate embeddings: %v", err)
	}
	fmt.Printf("Vector embeddings generated in %v\n", time.Since(stepStart))

	fmt.Printf("Total time taken: %v\n", time.Since(start))
}
