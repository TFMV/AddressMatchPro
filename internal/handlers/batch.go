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
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

		records, err := csv.NewReader(file).ReadAll()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Process each record
		for _, record := range records {
			req := matcher.MatchRequest{
				FirstName:   record[0],
				LastName:    record[1],
				PhoneNumber: record[2],
				Street:      record[3],
				City:        record[4],
				State:       record[5],
				ZipCode:     record[6],
				TopN:        10,
				RunID:       runID,
			}

			matcher.ProcessSingleRecord(pool, req)
		}

		// Generate TF/IDF vectors for the batch
		matcher.GenerateTFIDF(pool, runID)

		// Insert vector embeddings using Python script
		scriptPath := "./python-ml/generate_embeddings.py"
		if err := matcher.GenerateEmbeddingsPythonScript(scriptPath, runID); err != nil {
			http.Error(w, fmt.Sprintf("Failed to generate embeddings: %v", err), http.StatusInternalServerError)
			return
		}

		// Find matches for each record
		candidates := matcher.FindMatchesBatch(runID, matcher.NewScorer(), pool)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(candidates)
	}
}
