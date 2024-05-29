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
	"sort"

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
	ID          int    `json:"id"`
	ScriptPath  string `json:"script_path"`
}

// Candidate represents a potential match
type Candidate struct {
	MatchedCustomerID int     `json:"matched_customer_id"`
	Similarity        float64 `json:"similarity"`
}

// Scorer represents a scoring mechanism for candidates
type Scorer struct {
	Weights map[string]float64
}

// Score calculates a score based on the provided features and weights
func (s *Scorer) Score(features map[string]float64) float64 {
	score := 0.0
	for feature, weight := range s.Weights {
		score += features[feature] * weight
	}
	return score
}

// ExtractFeatures extracts features from the MatchRequest and Candidate
func ExtractFeatures(req MatchRequest, candidate Candidate, standardizedCandidateAddress string) map[string]float64 {
	features := make(map[string]float64)

	// Example feature: standardized address match
	if standardizedCandidateAddress == req.Street {
		features["address_match"] = 1.0
	} else {
		features["address_match"] = 0.0
	}

	// Add more features as needed

	return features
}

// FindMatches finds the best matches for a given MatchRequest
func FindMatches(req MatchRequest, scorer *Scorer, pool *pgxpool.Pool) []Candidate {
	log.Printf("Starting FindMatches for request: %+v\n", req)

	// Find potential matches based on binary key or vector similarity
	candidates, err := FindPotentialMatches(pool, req.RunID)
	if err != nil {
		log.Printf("Error finding potential matches: %v\n", err)
		return nil
	}

	log.Printf("Found %d potential matches\n", len(candidates))

	// Fetch details for candidates and score them
	for i, candidate := range candidates {
		var details struct {
			FirstName   string
			LastName    string
			PhoneNumber string
			Street      string
			City        string
			State       string
			ZipCode     string
		}

		err := pool.QueryRow(context.Background(), `
			SELECT first_name, last_name, phone_number, street, city, state, zip_code
			FROM customer_matching
			WHERE customer_id = $1 AND run_id = 0
		`, candidate.MatchedCustomerID).Scan(
			&details.FirstName,
			&details.LastName,
			&details.PhoneNumber,
			&details.Street,
			&details.City,
			&details.State,
			&details.ZipCode,
		)
		if err != nil {
			log.Printf("Error fetching candidate details: %v\n", err)
			continue
		}

		// Score the candidate
		standardizedCandidateAddress, err := StandardizeAddress(details.Street)
		if err != nil {
			log.Printf("Failed to standardize candidate address: %v\n", err)
			continue
		}

		features := ExtractFeatures(req, candidates[i], standardizedCandidateAddress)
		score := scorer.Score(features)
		candidates[i].Similarity = score

		log.Printf("Candidate %d scored: %f\n", candidate.MatchedCustomerID, score)
	}

	// Sort candidates by score in descending order
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Similarity > candidates[j].Similarity
	})

	// If the number of candidates exceeds the requested TopN, truncate the list
	if len(candidates) > req.TopN {
		candidates = candidates[:req.TopN]
	}

	log.Printf("Returning %d candidates\n", len(candidates))
	return candidates
}

// FindPotentialMatches finds potential matches based on binary key or vector similarity
func FindPotentialMatches(pool *pgxpool.Pool, runID int) ([]Candidate, error) {
	// SQL query to find potential matches
	query := `
	WITH embeddings AS (
		SELECT customer_id, vector_embedding
		FROM customer_vector_embedding
		WHERE run_id = $1
	),
	matching_embeddings AS (
		SELECT
			cv0.customer_id,
			cv0.vector_embedding,
			e.customer_id AS matched_customer_id,
			e.vector_embedding AS matched_vector_embedding,
			cv0.vector_embedding <=> e.vector_embedding AS similarity
		FROM
			customer_vector_embedding cv0
		JOIN
			embeddings e
		ON
			cv0.vector_embedding <=> e.vector_embedding <= 0.2 -- Adjusted threshold
		WHERE
			cv0.run_id = 0
	),
	matching_keys AS (
		SELECT
			ck0.customer_id,
			ck.customer_id AS matched_customer_id
		FROM
			customer_keys ck0
		JOIN
			customer_keys ck
		ON
			ck0.binary_key = ck.binary_key
		WHERE
			ck0.run_id = 0
			AND ck.run_id = $1
	),
	combined_matches AS (
		SELECT
			COALESCE(me.customer_id, mk.customer_id) AS customer_id,
			me.vector_embedding,
			COALESCE(me.matched_customer_id, mk.matched_customer_id) AS matched_customer_id,
			me.matched_vector_embedding,
			me.similarity
		FROM
			matching_embeddings me
		FULL OUTER JOIN
			matching_keys mk
		ON
			me.customer_id = mk.customer_id AND me.matched_customer_id = mk.matched_customer_id
	),
	ngram_sums AS (
		SELECT
			vt0.customer_id,
			SUM(vt0.ngram_tfidf) AS candidate_tfidf,
			SUM(vt.ngram_tfidf) AS matched_tfidf
		FROM
			customer_tokens vt0
		JOIN
			customer_tokens vt
		ON
			vt0.ngram_token = vt.ngram_token
			AND vt0.entity_type_id = vt.entity_type_id
		JOIN
			combined_matches cm
		ON
			vt0.customer_id = cm.customer_id
			AND vt.customer_id = cm.matched_customer_id
		WHERE
			vt0.run_id = 0
			AND vt.run_id = $1
		GROUP BY
			vt0.customer_id
	),
		 matches as (SELECT cm.customer_id,
							cm.vector_embedding,
							cm.matched_customer_id,
							cm.matched_vector_embedding,
							cm.similarity,
							ns.candidate_tfidf,
							ns.matched_tfidf
					 FROM combined_matches cm
							  LEFT JOIN
						  ngram_sums ns
						  ON
							  cm.customer_id = ns.customer_id)
	select m.customer_id as matched_customer_id,
		   m.similarity as similarity
	from matches m
	join customer_matching cm
	on cm.customer_id = m.customer_id
	ORDER BY
		m.similarity ASC NULLS LAST;
	`

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
		if err := rows.Scan(
			&candidate.MatchedCustomerID,
			&candidate.Similarity,
		); err != nil {
			return nil, err
		}
		log.Printf("Retrieved candidate: %+v\n", candidate) // Log each retrieved candidate
		candidates = append(candidates, candidate)
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("Total candidates found: %d\n", len(candidates)) // Log the total number of candidates found
	return candidates, nil
}

func FindMatchesBatch(runID int, scorer *Scorer, pool *pgxpool.Pool) []Candidate {
	return nil
}
