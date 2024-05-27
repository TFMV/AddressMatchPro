package matcher

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

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
	ID          int     `json:"id"`
	FullName    string  `json:"full_name"`
	Score       float64 `json:"score"`
	CustomerID  int
	Name        string
	Street      string
	Similarity  float64
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	PhoneNumber string `json:"phone_number"`
	City        string `json:"city"`
	State       string `json:"state"`
	ZipCode     string `json:"zip_code"`
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

	// Example feature: matching first name
	if strings.EqualFold(req.FirstName, candidate.FirstName) {
		features["first_name_match"] = 1.0
	} else {
		features["first_name_match"] = 0.0
	}

	// Example feature: matching last name
	if strings.EqualFold(req.LastName, candidate.LastName) {
		features["last_name_match"] = 1.0
	} else {
		features["last_name_match"] = 0.0
	}

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
	// Generate a new run ID for the request
	runID := req.RunID
	if runID == 0 {
		runID = CreateNewRun(pool, "Single Record Matching")
		req.RunID = runID
	}

	// Process the single record and generate embeddings
	ProcessSingleRecord(pool, req)
	err := generateEmbeddingsPythonScript(req.ScriptPath, runID)
	if err != nil {
		log.Printf("Error generating embeddings: %v\n", err)
		return nil
	}

	// Load reference entities
	referenceEntities := LoadReferenceEntities(pool)

	// Process customer addresses for the run ID
	ProcessCustomerAddresses(pool, referenceEntities, 10, runID)

	// Standardize the request address
	standardizedAddress, err := StandardizeAddress(req.Street)
	if err != nil {
		log.Printf("Failed to standardize address: %v\n", err)
		return nil
	}

	// Calculate the binary key for the request address
	binaryKey := CalculateBinaryKey(referenceEntities, strings.ToLower(standardizedAddress))

	// Find potential matches based on binary key or vector similarity
	candidates, err := FindPotentialMatches(pool, binaryKey, nil, runID)
	if err != nil {
		log.Printf("Error finding potential matches: %v\n", err)
		return nil
	}

	// Rank candidates based on composite score
	for i, candidate := range candidates {
		standardizedCandidateAddress, err := StandardizeAddress(candidate.Street)
		if err != nil {
			log.Printf("Failed to standardize candidate address: %v\n", err)
			continue
		}

		features := ExtractFeatures(req, candidate, standardizedCandidateAddress)
		score := scorer.Score(features)
		candidates[i].Score = score
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

// FindPotentialMatches finds potential matches based on binary key or vector similarity
func FindPotentialMatches(pool *pgxpool.Pool, binaryKey string, queryVector []float64, runID int) ([]Candidate, error) {
	// Convert the query vector to a string
	queryVectorStr := fmt.Sprintf("'{%s}'", join(queryVector, ", "))

	// SQL query to find potential matches
	query := `
		SELECT c.customer_id, c.customer_fname || ' ' || c.customer_lname AS name, c.customer_street AS street, cv.vector_embedding <=> $2 AS similarity
		FROM customer_vector_embedding cv
		JOIN customers c ON cv.customer_id = c.customer_id
		WHERE (c.binary_key = $1 OR cv.vector_embedding <=> $2 < 0.8) AND c.run_id = $3
		ORDER BY similarity ASC
		LIMIT 10;
	`

	// Execute the query
	rows, err := pool.Query(context.Background(), query, binaryKey, queryVectorStr, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []Candidate

	// Iterate through the rows and populate the candidates slice
	for rows.Next() {
		var candidate Candidate
		if err := rows.Scan(&candidate.CustomerID, &candidate.Name, &candidate.Street, &candidate.Similarity); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return candidates, nil
}

// FindMatchesBatch finds matches for a batch of records by run ID
func FindMatchesBatch(runID int, scorer *Scorer, pool *pgxpool.Pool) []Candidate {
	query := "SELECT customer_id, first_name, last_name, phone_number, street, city, state, zip_code FROM customer_matching WHERE run_id = $1"
	rows, err := pool.Query(context.Background(), query, runID)
	if err != nil {
		log.Printf("Query failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var candidates []Candidate
	for rows.Next() {
		var req MatchRequest
		err = rows.Scan(&req.ID, &req.FirstName, &req.LastName, &req.PhoneNumber, &req.Street, &req.City, &req.State, &req.ZipCode)
		if err != nil {
			log.Printf("Row scan failed: %v\n", err)
			continue
		}

		// Process each record
		standardizedAddress, err := StandardizeAddress(req.Street)
		if err != nil {
			log.Printf("Failed to standardize address: %v\n", err)
			continue
		}

		referenceEntities := LoadReferenceEntities(pool)
		binaryKey := CalculateBinaryKey(referenceEntities, strings.ToLower(standardizedAddress))

		candidateQuery := "SELECT customer_id, first_name, last_name, phone_number, street, city, state, zip_code FROM customer_keys WHERE binary_key = $1"
		candidateRows, err := pool.Query(context.Background(), candidateQuery, binaryKey)
		if err != nil {
			log.Printf("Candidate query failed: %v\n", err)
			continue
		}
		defer candidateRows.Close()

		for candidateRows.Next() {
			var candidate Candidate
			err = candidateRows.Scan(&candidate.CustomerID, &candidate.FirstName, &candidate.LastName, &candidate.PhoneNumber, &candidate.Street, &candidate.City, &candidate.State, &candidate.ZipCode)
			if err != nil {
				log.Printf("Candidate row scan failed: %v\n", err)
				continue
			}

			// Standardize candidate address
			standardizedCandidateAddress, err := StandardizeAddress(candidate.Street)
			if err != nil {
				log.Printf("Failed to standardize candidate address: %v\n", err)
				continue
			}

			features := ExtractFeatures(req, candidate, standardizedCandidateAddress)
			score := scorer.Score(features)
			candidate.Score = score

			candidates = append(candidates, candidate)
		}
	}

	// Sort candidates by score in descending order
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	return candidates
}
