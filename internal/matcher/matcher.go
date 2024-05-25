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
	standardizedAddress, err := standardizer.StandardizeAddress(
		req.FirstName, "", req.Street, req.City, req.State, req.ZipCode,
	)
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
		standardizedCandidateAddress, err := standardizer.StandardizeAddress(
			firstName, lastName, street, city, state, zipCode,
		)
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
