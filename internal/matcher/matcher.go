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
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/TFMV/FuzzyMatchFinder/internal/standardizer"
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
}

// Candidate represents a potential match
type Candidate struct {
	ID       int     `json:"id"`
	FullName string  `json:"full_name"`
	Score    float64 `json:"score"`
}

// FindMatches finds the best matches for a given MatchRequest
func FindMatches(req MatchRequest, scorer *Scorer, pool *pgxpool.Pool) []Candidate {
	standardizedAddress, err := standardizer.StandardizeAddress(req.Street)
	if err != nil {
		log.Printf("Failed to standardize address: %v\n", err)
		return nil
	}

	referenceEntities := LoadReferenceEntities(pool)
	binaryKey := CalculateBinaryKey(referenceEntities, strings.ToLower(standardizedAddress))

	query := "SELECT id, first_name, last_name, phone_number, street, city, state, zip_code FROM customers WHERE binary_key = $1"
	rows, err := pool.Query(context.Background(), query, binaryKey)
	if err != nil {
		log.Printf("Query failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var candidates []Candidate
	for rows.Next() {
		var id int
		var firstName, lastName, phoneNumber, street, city, state, zipCode string
		err = rows.Scan(&id, &firstName, &lastName, &phoneNumber, &street, &city, &state, &zipCode)
		if err != nil {
			log.Printf("Row scan failed: %v\n", err)
			continue
		}

		// Standardize candidate address
		standardizedCandidateAddress, err := standardizer.StandardizeAddress(street)
		if err != nil {
			log.Printf("Failed to standardize candidate address: %v\n", err)
			continue
		}

		features := ExtractFeatures(req, Candidate{
			ID:       id,
			FullName: fmt.Sprintf("%s %s", firstName, lastName),
		}, standardizedCandidateAddress)
		score := scorer.Score(features)
		candidates = append(candidates, Candidate{
			ID:       id,
			FullName: fmt.Sprintf("%s %s", firstName, lastName),
			Score:    score,
		})
	}

	// Sort candidates by score in descending order
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	if len(candidates) > req.TopN {
		candidates = candidates[:req.TopN]
	}

	return candidates
}
