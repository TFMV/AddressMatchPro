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

package handlers

import (
	"context"
	"fmt"
	"os"

	"github.com/TFMV/AddressMatchPro/internal/matcher"
	"github.com/TFMV/AddressMatchPro/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProcessBatch processes a batch CSV file and finds matches
func ProcessBatch(pool *pgxpool.Pool, filePath string, scriptPath string) ([]matcher.Candidate, error) {
	// Insert the records into the database with a unique run_id
	runID := matcher.CreateNewRun(pool, "Batch Record Matching")

	if err := utils.LoadCSV(pool, filePath); err != nil {
		return nil, fmt.Errorf("failed to load CSV: %v", err)
	}

	// Insert records from load_table into customer_matching with the given run_id
	err := matcher.InsertFromLoadTable(pool, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert records into customer_matching: %v", err)
	}

	// Generate TF/IDF vectors for the batch
	matcher.GenerateTFIDF(pool, runID)

	// Insert vector embeddings using Python script
	if err := matcher.GenerateEmbeddingsPythonScript(scriptPath, runID); err != nil {
		return nil, fmt.Errorf("failed to generate embeddings: %v", err)
	}

	// Fetch all unique customer IDs for the given run ID
	customerIDs, err := matcher.GetCustomerIDs(pool, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get customer IDs: %v", err)
	}

	var allCandidates []matcher.Candidate
	for _, customerID := range customerIDs {
		req := matcher.MatchRequest{
			RunID: runID,
			ID:    customerID,
			TopN:  10,
		}

		candidates := matcher.FindMatches(req, pool)
		allCandidates = append(allCandidates, candidates...)
	}

	return allCandidates, nil
}

// Main function to run batch processing standalone
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: batch <csv_file_path> <python_script_path>")
		return
	}

	csvFilePath := os.Args[1]
	pythonScriptPath := os.Args[2]

	// Load the configuration
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/config.yaml"
	}
	config, err := matcher.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	// Create the database connection string
	databaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		config.DBCreds.Username,
		config.DBCreds.Password,
		config.DBCreds.Host,
		config.DBCreds.Port,
		config.DBCreds.Database,
	)

	// Create the connection pool
	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		fmt.Printf("Unable to create connection pool: %v\n", err)
		return
	}
	defer pool.Close()

	candidates, err := ProcessBatch(pool, csvFilePath, pythonScriptPath)
	if err != nil {
		fmt.Printf("Error processing batch: %v\n", err)
		return
	}

	// Output the results
	for _, candidate := range candidates {
		fmt.Printf("Matched Customer ID: %d, Score: %.2f\n", candidate.MatchedCustomerID, candidate.Score)
	}
}
