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

package matcher

import (
	"context"
	"log"
	"math"
	"os"
	"sort"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MatchRequest represents a matching request
type MatchRequest struct {
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	PhoneNumber string `json:"phone_number"`
	Street      string `json:"street"`
	City        string `json:"city"`
	State       string `json:"state"`
	ZipCode     string `json:"zip_code"`
	TopN        int    `json:"top_n"`
	RunID       int    `json:"run_id"`
	ScriptPath  string `json:"script_path"`
}

// Candidate represents a potential match
type Candidate struct {
	InputCustomerID          int     `json:"input_customer_id"`
	InputRunID               int     `json:"input_run_id"`
	InputFirstName           string  `json:"input_first_name"`
	InputLastName            string  `json:"input_last_name"`
	InputStreet              string  `json:"input_street"`
	InputCity                string  `json:"input_city"`
	InputState               string  `json:"input_state"`
	InputZipCode             string  `json:"input_zip_code"`
	InputPhoneNumber         string  `json:"input_phone_number"`
	CandidateCustomerID      int     `json:"candidate_customer_id"`
	CandidateRunID           int     `json:"candidate_run_id"`
	CandidateFirstName       string  `json:"candidate_first_name"`
	CandidateLastName        string  `json:"candidate_last_name"`
	CandidateStreet          string  `json:"candidate_street"`
	CandidateCity            string  `json:"candidate_city"`
	CandidateState           string  `json:"candidate_state"`
	CandidateZipCode         string  `json:"candidate_zip_code"`
	CandidatePhoneNumber     string  `json:"candidate_phone_number"`
	Similarity               float64 `json:"similarity"`
	BinKeyMatch              bool    `json:"bin_key_match"`
	TfidfScore               float64 `json:"tfidf_score"`
	Rank                     int     `json:"rank"`
	Score                    float64 `json:"score"`
	TrigramCosineFirstName   float64 `json:"trigram_cosine_first_name"`
	TrigramCosineLastName    float64 `json:"trigram_cosine_last_name"`
	TrigramCosineStreet      float64 `json:"trigram_cosine_street"`
	TrigramCosineCity        float64 `json:"trigram_cosine_city"`
	TrigramCosinePhoneNumber float64 `json:"trigram_cosine_phone_number"`
	TrigramCosineZipCode     float64 `json:"trigram_cosine_zip_code"`
}

// LoadSQLQuery loads an SQL query from a file
func LoadSQLQuery(filepath string) (string, error) {
	queryBytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	return string(queryBytes), nil
}

// FindPotentialMatches finds potential matches and scores them based on composite score
func FindPotentialMatches(pool *pgxpool.Pool, runID int, topN int) ([]Candidate, error) {
	query, err := LoadSQLQuery("/Users/thomasmcgeehan/AddressMatchPro/AddressMatchPro/internal/matcher/match.sql")
	if err != nil {
		return nil, err
	}

	// Log the query and the runID parameter
	log.Printf("Executing query with runID: %d\nQuery: %s\n", runID, query)

	// Execute the query
	rows, err := pool.Query(context.Background(), query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []Candidate

	// Iterate through the rows and populate the candidates slice
	for rows.Next() {
		var candidate Candidate
		var inputFirstName, inputLastName, inputPhoneNumber, inputStreet, inputCity, inputState, inputZipCode pgtype.Text
		var candidateFirstName, candidateLastName, candidatePhoneNumber, candidateStreet, candidateCity, candidateState, candidateZipCode pgtype.Text
		var binKeyMatch pgtype.Bool
		var rank pgtype.Int4

		if err := rows.Scan(
			&candidate.InputCustomerID,
			&candidate.InputRunID,
			&inputFirstName,
			&inputLastName,
			&inputStreet,
			&inputCity,
			&inputState,
			&inputZipCode,
			&inputPhoneNumber,
			&candidate.CandidateCustomerID,
			&candidate.CandidateRunID,
			&candidateFirstName,
			&candidateLastName,
			&candidateStreet,
			&candidateCity,
			&candidateState,
			&candidateZipCode,
			&candidatePhoneNumber,
			&candidate.Similarity,
			&binKeyMatch,
			&candidate.TfidfScore,
			&rank,
		); err != nil {
			return nil, err
		}

		// Convert pgtype.Text to string
		candidate.InputFirstName = inputFirstName.String
		candidate.InputLastName = inputLastName.String
		candidate.InputPhoneNumber = inputPhoneNumber.String
		candidate.InputStreet = inputStreet.String
		candidate.InputCity = inputCity.String
		candidate.InputState = inputState.String
		candidate.InputZipCode = inputZipCode.String
		candidate.CandidateFirstName = candidateFirstName.String
		candidate.CandidateLastName = candidateLastName.String
		candidate.CandidatePhoneNumber = candidatePhoneNumber.String
		candidate.CandidateStreet = candidateStreet.String
		candidate.CandidateCity = candidateCity.String
		candidate.CandidateState = candidateState.String
		candidate.CandidateZipCode = candidateZipCode.String
		candidate.BinKeyMatch = binKeyMatch.Bool
		candidate.Rank = int(rank.Int32)

		// Calculate n-gram similarities
		candidate.TrigramCosineFirstName = ngramFrequencySimilarity(candidate.InputFirstName, candidate.CandidateFirstName, 2)
		candidate.TrigramCosineLastName = ngramFrequencySimilarity(candidate.InputLastName, candidate.CandidateLastName, 2)
		candidate.TrigramCosineStreet = ngramFrequencySimilarity(candidate.InputStreet, candidate.CandidateStreet, 2)
		candidate.TrigramCosineCity = ngramFrequencySimilarity(candidate.InputCity, candidate.CandidateCity, 2)
		candidate.TrigramCosinePhoneNumber = ngramFrequencySimilarity(candidate.InputPhoneNumber, candidate.CandidatePhoneNumber, 2)
		candidate.TrigramCosineZipCode = ngramFrequencySimilarity(candidate.InputZipCode, candidate.CandidateZipCode, 2)

		// Compute the normalized similarity score
		normalizedSimilarity := 1 - candidate.Similarity

		// Define weights for each feature based on its importance
		weights := map[string]float64{
			"similarity":  0.25,
			"tfidf":       0.2,
			"firstName":   0.1,
			"lastName":    0.1,
			"street":      0.1,
			"city":        0.1,
			"phoneNumber": 0.05,
			"zipCode":     0.05,
			"binKeyMatch": 0.05,
		}

		// Adjust composite score based on binary key match
		binKeyMatchScore := 0.0
		if candidate.BinKeyMatch {
			binKeyMatchScore = 1.0
		}

		// Calculate composite score based on weighted features
		compositeScore := normalizedSimilarity*weights["similarity"] + candidate.TfidfScore*weights["tfidf"] +
			candidate.TrigramCosineFirstName*weights["firstName"] + candidate.TrigramCosineLastName*weights["lastName"] +
			candidate.TrigramCosineStreet*weights["street"] + candidate.TrigramCosineCity*weights["city"] +
			candidate.TrigramCosinePhoneNumber*weights["phoneNumber"] + candidate.TrigramCosineZipCode*weights["zipCode"] +
			binKeyMatchScore*weights["binKeyMatch"]

		candidate.Score = math.Max(1, math.Min(100, compositeScore*100))

		candidates = append(candidates, candidate)
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("Total candidates found: %d\n", len(candidates)) // Log the total number of candidates found

	// Sort candidates by score in descending order
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	// If the number of candidates exceeds the requested TopN, truncate the list
	if len(candidates) > topN {
		candidates = candidates[:topN]
	}

	return candidates, nil
}
