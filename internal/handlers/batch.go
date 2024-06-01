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
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/TFMV/AddressMatchPro/internal/matcher"
	"github.com/TFMV/AddressMatchPro/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MatchBatchHandler handles batch record matching
func MatchBatchHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Insert the records into the database with a unique run_id
		runID := matcher.CreateNewRun(pool, "Batch Record Matching")

		tempFile, err := os.CreateTemp("", "batch-upload-*.csv")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create temp file: %v", err), http.StatusInternalServerError)
			return
		}
		defer os.Remove(tempFile.Name())

		if _, err := tempFile.ReadFrom(file); err != nil {
			http.Error(w, fmt.Sprintf("Failed to read file: %v", err), http.StatusInternalServerError)
			return
		}

		if err := utils.LoadCSV(pool, tempFile.Name()); err != nil {
			http.Error(w, fmt.Sprintf("Failed to load CSV: %v", err), http.StatusInternalServerError)
			return
		}

		// Insert records from load_table into customer_matching with the given run_id
		err = matcher.InsertFromLoadTable(pool, runID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to insert records into customer_matching: %v", err), http.StatusInternalServerError)
			return
		}

		// Load reference entities
		referenceEntities := matcher.LoadReferenceEntities(pool)

		// Process customer addresses and generate binary keys
		matcher.ProcessCustomerAddresses(pool, referenceEntities, 10, runID)

		// Generate TF/IDF vectors for the batch
		matcher.GenerateTFIDF(pool, runID)

		// Insert vector embeddings using Python script
		scriptPath := "./python-ml/generate_embeddings.py"
		if err := matcher.GenerateEmbeddingsPythonScript(scriptPath, runID); err != nil {
			http.Error(w, fmt.Sprintf("Failed to generate embeddings: %v", err), http.StatusInternalServerError)
			return
		}

		// Run the match query for the entire space of records within the run_id
		candidates, err := matcher.FindPotentialMatches(pool, runID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to find matches: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(candidates)
	}
}
