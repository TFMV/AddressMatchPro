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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING WITHOUT LIMITATION THE WARRANTIES OF MERCHANTABILITY,
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

package api

import (
	"encoding/json"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"os"

	"github.com/TFMV/AddressMatchPro/internal/matcher"
	"github.com/TFMV/AddressMatchPro/pkg/utils"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HealthCheckHandler returns a simple health check response
func HealthCheckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}

// MatchHandler handles both single and batch match requests
func MatchHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Println("Entering MatchHandler")
		var req matcher.MatchRequest
		isBatch := false

		// Check if it's a batch request by looking for a file upload
		file, err := c.FormFile("file")
		if err == nil {
			isBatch = true
		}

		if isBatch {
			handleBatchMatch(c, pool, file)
		} else {
			log.Println("Processing single match request")
			if err := c.ShouldBindJSON(&req); err != nil {
				log.Printf("Error binding JSON: %v", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			log.Printf("MatchRequest: %+v", req)
			handleSingleMatch(c, pool, req)
		}
	}
}

// MatchDuplicates handles duplicate match requests
func MatchDuplicates(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req matcher.MatchRequest
		if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Match Duplicates supports matching the candidate space (run_id = 0) to itself
		req.RunID = 0

		// Find matches
		candidates, err := matcher.FindPotentialMatches(pool, req.RunID, req.TopN)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to find matches: %v", err)})
			return
		}

		c.JSON(http.StatusOK, candidates)
	}
}

func handleSingleMatch(c *gin.Context, pool *pgxpool.Pool, req matcher.MatchRequest) {
	log.Println("Handling single match")
	// Insert the single record into the database with a unique run_id
	runID := matcher.CreateNewRun(pool, "Single Record Matching")
	req.RunID = runID
	req.ScriptPath = "/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/python-ml/generate_embeddings.py" // Set the script path

	// Process the single record
	if err := matcher.ProcessSingleRecord(pool, req); err != nil {
		log.Printf("Failed to insert single record: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert single record: %v", err)})
		return
	}

	processAndMatch(pool, runID, req.TopN, 1, c)
}

func handleBatchMatch(c *gin.Context, pool *pgxpool.Pool, file *multipart.FileHeader) {
	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to open file: %v", err)})
		return
	}
	defer f.Close()

	// Insert the records into the database with a unique run_id
	runID := matcher.CreateNewRun(pool, "Batch Record Matching")

	tempFile, err := os.CreateTemp("", "batch-upload-*.csv")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to create temp file: %v", err)})
		return
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.ReadFrom(f); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to read file: %v", err)})
		return
	}

	// truncate batch_match table
	if err := matcher.TruncateBatchMatchTable(pool); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to truncate batch_match: %v", err)})
		return
	}

	if err := utils.LoadCSV(pool, tempFile.Name()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load CSV: %v", err)})
		return
	}

	// Insert records from load_table into customer_matching with the given run_id
	err = matcher.InsertFromLoadTable(pool, runID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert records into customer_matching: %v", err)})
		return
	}

	processAndMatch(pool, runID, 10, 10, c)
}

func processAndMatch(pool *pgxpool.Pool, runID int, topN int, workers int, c *gin.Context) {
	log.Println("Processing and matching")
	// Load reference entities once
	referenceEntities := matcher.LoadReferenceEntities(pool)

	// Process customer addresses and generate binary keys with concurrency
	matcher.ProcessCustomerAddresses(pool, referenceEntities, workers, runID)

	// Generate TF/IDF vectors
	matcher.GenerateTFIDF(pool, runID)

	// Insert vector embeddings using Python script
	scriptPath := "/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/python-ml/generate_embeddings.py"
	if err := matcher.GenerateEmbeddingsPythonScript(scriptPath, runID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to generate embeddings: %v", err)})
		return
	}

	// Find matches
	candidates, err := matcher.FindPotentialMatches(pool, runID, topN)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to find matches: %v", err)})
		return
	}

	c.JSON(http.StatusOK, candidates)
}

