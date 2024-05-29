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
	ID          int    `json:"id"`
	ScriptPath  string `json:"script_path"`
}

// Candidate represents a potential match
type Candidate struct {
	MatchedCustomerID int     `json:"matched_customer_id"`
	Similarity        float64 `json:"similarity"`
	FirstName         string  `json:"first_name"`
	LastName          string  `json:"last_name"`
	PhoneNumber       string  `json:"phone_number"`
	Street            string  `json:"street"`
	City              string  `json:"city"`
	State             string  `json:"state"`
	ZipCode           string  `json:"zip_code"`
	MatchedTFIDF      float64 `json:"matched_tfidf"`
	Score             float64 `json:"score"`
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
	// Normalize score to be between 1 and 100
	return math.Max(1, math.Min(100, score*100))
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

	// Example feature: phone number match
	if candidate.PhoneNumber == req.PhoneNumber {
		features["phone_number_match"] = 1.0
	} else {
		features["phone_number_match"] = 0.0
	}

	// Example feature: name match
	if candidate.FirstName == req.FirstName && candidate.LastName == req.LastName {
		features["name_match"] = 1.0
	} else {
		features["name_match"] = 0.0
	}

	// Example feature: city match
	if candidate.City == req.City {
		features["city_match"] = 1.0
	} else {
		features["city_match"] = 0.0
	}

	// Example feature: state match
	if candidate.State == req.State {
		features["state_match"] = 1.0
	} else {
		features["state_match"] = 0.0
	}

	// Example feature: zip code match
	if candidate.ZipCode == req.ZipCode {
		features["zip_code_match"] = 1.0
	} else {
		features["zip_code_match"] = 0.0
	}

	// Feature: similarity
	features["similarity"] = candidate.Similarity

	// Feature: TFIDF score
	features["tfidf_score"] = candidate.MatchedTFIDF

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

	// Rank candidates based on composite score
	for i, candidate := range candidates {
		standardizedCandidateAddress, err := StandardizeAddress(candidate.Street)
		if err != nil {
			log.Printf("Failed to standardize candidate address for candidate %d: %v\n", candidate.MatchedCustomerID, err)
			continue
		}

		features := ExtractFeatures(req, candidate, standardizedCandidateAddress)
		score := scorer.Score(features)
		candidates[i].Score = score

		log.Printf("Candidate %d scored: %f\n", candidate.MatchedCustomerID, score)
	}

	// Sort candidates by score in descending order
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
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
	matches AS (
		SELECT 
			cm.customer_id,
			cm.vector_embedding,
			cm.matched_customer_id,
			cm.matched_vector_embedding,
			cm.similarity,
			ns.candidate_tfidf,
			ns.matched_tfidf
		FROM combined_matches cm
		LEFT JOIN ngram_sums ns ON cm.customer_id = ns.customer_id
	)
	SELECT 
		cm.customer_id as matched_customer_id,
		case when m.similarity is null then 0 else m.similarity end as similarity,
		COALESCE(cm.first_name, '') AS first_name,
		COALESCE(cm.last_name, '') AS last_name,
		COALESCE(cm.phone_number, '') AS phone_number,
		COALESCE(cm.street, '') AS street,
		COALESCE(cm.city, '') AS city,
		COALESCE(cm.state, '') AS state,
		COALESCE(cm.zip_code, '') AS zip_code,
		case when m.similarity is null then 0 else m.similarity end as similarity,
		case when m.matched_tfidf is null then 0 else m.matched_tfidf end as matched_tfidf
	FROM matches m
	JOIN customer_matching cm ON cm.customer_id = m.customer_id
	WHERE cm.run_id = 0 AND EXISTS (
		SELECT 1
		FROM customer_matching cm2
		WHERE cm2.run_id = 0 AND
			  (cm2.state = cm.state OR cm2.zip_code = cm.zip_code) AND
			  (cm2.zip_code = cm.zip_code OR
			   cm2.city = cm.city OR
			   cm2.phone_number = cm.phone_number) AND
			  cm2.customer_id = cm.customer_id
	)
	ORDER BY m.similarity ASC, m.matched_tfidf DESC NULLS LAST
	LIMIT 10;
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
		var firstName, lastName, phoneNumber, street, city, state, zipCode pgtype.Text

		if err := rows.Scan(
			&candidate.MatchedCustomerID,
			&candidate.Similarity,
			&firstName,
			&lastName,
			&phoneNumber,
			&street,
			&city,
			&state,
			&zipCode,
			&candidate.Similarity,
			&candidate.MatchedTFIDF,
		); err != nil {
			return nil, err
		}

		// Convert pgtype.Text to string
		candidate.FirstName = firstName.String
		candidate.LastName = lastName.String
		candidate.PhoneNumber = phoneNumber.String
		candidate.Street = street.String
		candidate.City = city.String
		candidate.State = state.String
		candidate.ZipCode = zipCode.String

		candidates = append(candidates, candidate)
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("Total candidates found: %d\n", len(candidates)) // Log the total number of candidates found
	return candidates, nil
}
