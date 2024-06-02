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
	ScriptPath  string `json:"script_path"`
}

// Candidate represents a potential match
type Candidate struct {
	InputCustomerID      int     `json:"input_customer_id"`
	InputRunID           int     `json:"input_run_id"`
	InputFirstName       string  `json:"input_first_name"`
	InputLastName        string  `json:"input_last_name"`
	InputStreet          string  `json:"input_street"`
	InputCity            string  `json:"input_city"`
	InputState           string  `json:"input_state"`
	InputZipCode         string  `json:"input_zip_code"`
	InputPhoneNumber     string  `json:"input_phone_number"`
	CandidateCustomerID  int     `json:"candidate_customer_id"`
	CandidateRunID       int     `json:"candidate_run_id"`
	CandidateFirstName   string  `json:"candidate_first_name"`
	CandidateLastName    string  `json:"candidate_last_name"`
	CandidateStreet      string  `json:"candidate_street"`
	CandidateCity        string  `json:"candidate_city"`
	CandidateState       string  `json:"candidate_state"`
	CandidateZipCode     string  `json:"candidate_zip_code"`
	CandidatePhoneNumber string  `json:"candidate_phone_number"`
	Similarity           float64 `json:"similarity"`
	BinKeyMatch          bool    `json:"bin_key_match"`
	TfidfScore           float64 `json:"tfidf_score"`
	Rank                 int     `json:"rank"`
	Score                float64 `json:"score"`
}

// FindPotentialMatches finds potential matches and scores them based on composite score
func FindPotentialMatches(pool *pgxpool.Pool, runID int, topN int) ([]Candidate, error) {
	// SQL query to find potential matches
	query := `
	with matches as (
		select input.customer_id as input_customer_id,
			input.run_id as input_run_id,
			input.first_name as input_first_name,
			input.last_name as input_last_name,
			input.street as input_street,
			input.city as input_city,
			input.state as input_state,
			input.zip_code as input_zip_code,
			input.phone_number as input_phone_number,
			candidates.customer_id as candidate_customer_id,
			candidates.run_id as candidate_run_id,
			candidates.first_name as candidate_first_name,
			candidates.last_name as candidate_last_name,
			candidates.street as candidate_street,
			candidates.city as candidate_city,
			candidates.state as candidate_state,
			candidates.zip_code as candidate_zip_code,
			candidates.phone_number as candidate_phone_number,
			candidate_vec.vector_embedding <=> input_vec.vector_embedding AS similarity
		from customer_matching candidates
		join customer_matching input
		   on ((candidates.state = input.state OR
				candidates.zip_code = input.zip_code) and
			   (candidates.zip_code = input.zip_code OR
				candidates.city = input.city OR
				candidates.phone_number = input.phone_number)
			   )
		join customer_vector_embedding candidate_vec
		   on (candidate_vec.customer_id = candidates.customer_id and
			   candidate_vec.run_id = candidates.run_id)
		join customer_vector_embedding input_vec
		   on (input_vec.customer_id = input.customer_id and
			   input_vec.run_id = input.run_id)
		where candidates.run_id = 0
		and input.run_id = $1),
		bin_keys as (
			select input.customer_id as input_customer_id,
				   match.customer_id as match_customer_id
			from customer_keys input
			join customer_keys match
			on (input.binary_key = match.binary_key)
			join matches
			on (matches.input_customer_id = input.customer_id and
				matches.candidate_customer_id = match.customer_id)
		)
		select coalesce(matches.input_customer_id, 0),
			   coalesce(matches.input_run_id, 0),
			   coalesce(matches.input_first_name, ''),
			   coalesce(matches.input_last_name, ''),
			   coalesce(matches.input_street, ''),
			   coalesce(matches.input_city, ''),
			   coalesce(matches.input_state, ''),
			   coalesce(matches.input_zip_code, ''),
			   coalesce(matches.input_phone_number, ''),
			   coalesce(matches.candidate_customer_id, 0),
			   coalesce(matches.candidate_run_id, 0),
			   coalesce(matches.candidate_first_name, ''),
			   coalesce(matches.candidate_last_name, ''),
			   coalesce(matches.candidate_street, ''),
			   coalesce(matches.candidate_city, ''),
			   coalesce(matches.candidate_state, ''),
			   coalesce(matches.candidate_zip_code, ''),
			   coalesce(matches.candidate_phone_number, ''),
			   coalesce(matches.similarity, 100) as similarity,
			   case when bin_keys.match_customer_id is null then false else true end as bin_key_match,
			   sum(coalesce(input_tfidf.ngram_tfidf, 0) * coalesce(candidate_tfidf.ngram_tfidf, 0)) as tfidf_score,
			   rank() over (partition by matches.input_customer_id order by coalesce(matches.similarity, 100)) as rank
		from matches
		join customer_tokens input_tfidf
		on (input_tfidf.run_id = matches.input_run_id and
			input_tfidf.customer_id = matches.input_customer_id)
		join customer_tokens candidate_tfidf
		on (candidate_tfidf.run_id = matches.candidate_run_id and
			candidate_tfidf.customer_id = matches.candidate_customer_id and
			candidate_tfidf.entity_type_id = input_tfidf.entity_type_id and
		   candidate_tfidf.ngram_token = input_tfidf.ngram_token)
		left outer join bin_keys
		on (bin_keys.input_customer_id = matches.input_customer_id and
			bin_keys.match_customer_id = matches.candidate_customer_id)
		where matches.similarity <= .1
		group by matches.input_customer_id,
			   matches.input_run_id,
			   matches.input_first_name,
			   matches.input_last_name,
			   matches.input_street,
			   matches.input_city,
			   matches.input_state,
			   matches.input_zip_code,
			   matches.input_phone_number,
			   matches.candidate_customer_id,
			   matches.candidate_run_id,
			   matches.candidate_first_name,
			   matches.candidate_last_name,
			   matches.candidate_street,
			   matches.candidate_city,
			   matches.candidate_state,
			   matches.candidate_zip_code,
			   matches.candidate_phone_number,
			   matches.similarity,
			   case when bin_keys.match_customer_id is null then false else true end
		order by matches.input_customer_id, matches.similarity;
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

		// Compute the score as 80% vector similarity and 20% n-gram TFIDF score
		score := 0.8*candidate.Similarity + 0.2*candidate.TfidfScore
		candidate.Score = math.Max(1, math.Min(100, score*100))

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
