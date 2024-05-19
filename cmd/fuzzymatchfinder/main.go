package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/agnivade/levenshtein"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
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

func main() {
	var err error
	dbpool, err = pgxpool.Connect(context.Background(), "postgres://user:password@localhost:5432/mydb")
	if err != nil {
		fmt.Printf("Unable to connect to database: %v\n", err)
		return
	}
	defer dbpool.Close()

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

	matches := findMatches(req)
	c.JSON(http.StatusOK, MatchResponse{Matches: matches})
}

func findMatches(req MatchRequest) []Candidate {
	query := "SELECT id, first_name, last_name, phone_number, street, city, state, zip_code FROM my_table"
	rows, err := dbpool.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var candidates []Candidate
	for rows.Next() {
		var id int
		var firstName, lastName, phoneNumber, street, city, state, zipCode string
		err = rows.Scan(&id, &firstName, &lastName, &phoneNumber, &street, &city, &state, &zipCode)
		if err != nil {
			fmt.Printf("Row scan failed: %v\n", err)
			continue
		}

		score := calculateScore(req, firstName, lastName, phoneNumber, street, city, state, zipCode)
		candidates = append(candidates, Candidate{
			ID:       id,
			FullName: fmt.Sprintf("%s %s", firstName, lastName),
			Score:    score,
		})
	}

	// Sort and return top N candidates
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > req.TopN {
		candidates = candidates[:req.TopN]
	}
	return candidates
}

func calculateScore(req MatchRequest, firstName, lastName, phoneNumber, street, city, state, zipCode string) float64 {
	score := 0.0
	if req.FirstName != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.FirstName, firstName)))
	}
	if req.LastName != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.LastName, lastName)))
	}
	if req.PhoneNumber != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.PhoneNumber, phoneNumber)))
	}
	if req.Street != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.Street, street)))
	}
	if req.City != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.City, city)))
	}
	if req.State != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.State, state)))
	}
	if req.ZipCode != "" {
		score += 1.0 / (1.0 + float64(levenshtein.ComputeDistance(req.ZipCode, zipCode)))
	}
	return score
}
