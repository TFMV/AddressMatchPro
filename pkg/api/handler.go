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

package api

import (
	"encoding/csv"
	"net/http"
	"time"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/TFMV/FuzzyMatchFinder/pkg/utils"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MatchSingleHandler handles single match requests
func MatchSingleHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req matcher.MatchRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			utils.SendError(c.Writer, http.StatusBadRequest, err)
			return
		}

		// Insert the single record into the database with a unique run_id
		runID := matcher.CreateNewRun(pool, "Single Record Matching")
		req.RunID = runID

		// Process the record and generate keys/vectors
		matcher.ProcessSingleRecord(pool, req)

		// Find matches
		candidates := matcher.FindMatches(req, matcher.NewScorer(), pool)

		utils.SendJSON(c.Writer, http.StatusOK, "Matches found successfully", candidates)
	}
}

// MatchBatchHandler handles batch match requests
func MatchBatchHandler(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		file, err := c.FormFile("file")
		if err != nil {
			utils.SendError(c.Writer, http.StatusBadRequest, err)
			return
		}

		f, err := file.Open()
		if err != nil {
			utils.SendError(c.Writer, http.StatusInternalServerError, err)
			return
		}
		defer f.Close()

		// Insert the records into the database with a unique run_id
		runID := matcher.CreateNewRun(pool, "Batch Record Matching")

		records, err := csv.NewReader(f).ReadAll()
		if err != nil {
			utils.SendError(c.Writer, http.StatusInternalServerError, err)
			return
		}

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

			// Process the record and generate keys/vectors
			matcher.ProcessSingleRecord(pool, req)
		}

		// Find matches for each record
		candidates := matcher.FindMatchesBatch(runID, matcher.NewScorer(), pool)

		utils.SendJSON(c.Writer, http.StatusOK, "Batch matches found successfully", candidates)
	}
}

// HealthCheckHandler handles health check requests
func HealthCheckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		zuluTime := time.Now().UTC().Format(time.RFC3339)
		c.JSON(http.StatusOK, gin.H{
			"status":   "OK",
			"zuluTime": zuluTime,
		})
	}
}
