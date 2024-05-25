package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/TFMV/FuzzyMatchFinder/standardizer"
)

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

type MatchResponse struct {
	Matches []Candidate `json:"matches"`
}

type Candidate struct {
	ID       int     `json:"id"`
	FullName string  `json:"full_name"`
	Score    float64 `json:"score"`
}

var dbpool *pgxpool.Pool
var scorer *matcher.Scorer

func main() {
	var err error
	dbpool, err = pgxpool.Connect(context.Background(), "postgres://user:password@localhost:5432/mydb")
	if err != nil {
		fmt.Printf("Unable to connect to database: %v\n", err)
		return
	}
	defer dbpool.Close()

	scorer, err = matcher.LoadModel("scorer_model.pkl")
	if err != nil {
		fmt.Printf("Unable to load model: %v\n", err)
		return
	}

	r := gin.Default()
	r.POST("/match-entity", matchEntityHandler)
	port := "8080"
	r.Run(":" + port)
}

func matchEntityHandler(c *gin.Context) {
	var req MatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Standardize the address before finding matches
	standardizedAddress, err := standardizer.StandardizeAddress(
		req.FirstName, req.LastName, req.PhoneNumber, req.Street, req.City, req.State, req.ZipCode,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to standardize address"})
		return
	}

	// Split the standardized address into components
	components := strings.Fields(standardizedAddress)
	if len(components) >= 6 {
		req.Street = strings.Join(components[2:len(components)-4], " ")
		req.City = components[len(components)-4]
		req.State = components[len(components)-3]
		req.ZipCode = components[len(components)-2]
	}

	matches := findMatches(req)
	c.JSON(http.StatusOK, MatchResponse{Matches: matches})
}

func findMatches(req MatchRequest) []Candidate {
	query := "SELECT id, first_name, last_name, phone_number, street, city, state, zip_code FROM customers"
	rows, err := dbpool.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var candidates []Candidate
	var mu sync.Mutex
	var wg sync.WaitGroup

	for rows.Next() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var id int
			var firstName, lastName, phoneNumber, street, city, state, zipCode string
			err := rows.Scan(&id, &firstName, &lastName, &phoneNumber, &street, &city, &state, &zipCode)
			if err != nil {
				fmt.Printf("Row scan failed: %v\n", err)
				return
			}

			// Standardize candidate address
			standardizedCandidateAddress, err := standardizer.StandardizeAddress(
				firstName, lastName, phoneNumber, street, city, state, zipCode,
			)
			if err != nil {
				fmt.Printf("Failed to standardize candidate address: %v\n", err)
				return
			}

			score := calculateScore(req, standardizedCandidateAddress)
			candidate := Candidate{
				ID:       id,
				FullName: fmt.Sprintf("%s %s", firstName, lastName),
				Score:    score,
			}

			mu.Lock()
			candidates = append(candidates, candidate)
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Sort and return top N candidates
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > req.TopN {
		candidates = candidates[:req.TopN]
	}
	return candidates
}

func calculateScore(req MatchRequest, standardizedCandidateAddress string) float64 {
	score := 0.0
	standardizedReqAddress, err := standardizer.StandardizeAddress(
		req.FirstName, req.LastName, req.PhoneNumber, req.Street, req.City, req.State, req.ZipCode,
	)
	if err != nil {
		fmt.Printf("Failed to standardize request address: %v\n", err)
		return score
	}

	score += 1.0 / (1.0 + float64(matcher.CosineSimilarity(standardizedReqAddress, standardizedCandidateAddress)))
	return score
}
