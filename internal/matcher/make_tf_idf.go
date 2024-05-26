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
	"sync"

	"github.com/TFMV/FuzzyMatchFinder/internal/standardizer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jdkato/prose/v2"
)

func standardizeStreet(street string) string {
	standardizedStreet, err := standardizer.StandardizeAddress(street)
	if err != nil {
		log.Fatalf("Error standardizing street: %v", err)
	}
	return standardizedStreet
}

func tokenize(text string) []string {
	doc, err := prose.NewDocument(text)
	if err != nil {
		log.Fatal(err)
	}
	tokens := []string{}
	for _, tok := range doc.Tokens() {
		tokens = append(tokens, tok.Text)
	}
	return tokens
}

func calculateIDF(totalDocs int, docFreq map[string]int) map[string]float64 {
	idf := make(map[string]float64)
	for token, freq := range docFreq {
		idf[token] = math.Log(float64(totalDocs) / float64(freq))
	}
	return idf
}

func GenerateTFIDF(pool *pgxpool.Pool) {
	rows, err := pool.Query(context.Background(), "SELECT customer_id as id, lower(customer_fname) || ' ' || lower(customer_lname) as name, lower(customer_street) as street FROM customers")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	type Customer struct {
		ID     int
		Name   string
		Street string
	}

	var customers []Customer
	for rows.Next() {
		var id int
		var name, street string
		if err := rows.Scan(&id, &name, &street); err != nil {
			log.Fatal(err)
		}
		customers = append(customers, Customer{
			ID:     id,
			Name:   name,
			Street: standardizeStreet(street),
		})
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	totalDocs := len(customers)
	docFreqName := make(map[string]int)
	docFreqStreet := make(map[string]int)
	customerTokens := []struct {
		CustomerID int
		EntityType int
		Token      string
		Frequency  int
	}{}

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit to 10 concurrent goroutines

	for _, customer := range customers {
		wg.Add(1)
		go func(c Customer) {
			defer wg.Done()
			sem <- struct{}{}

			nameTokenFreq := make(map[string]int)
			streetTokenFreq := make(map[string]int)

			// Tokenize and count tokens for name
			nameTokens := tokenize(c.Name)
			for _, token := range nameTokens {
				nameTokenFreq[token]++
			}

			// Tokenize and count tokens for street
			streetTokens := tokenize(c.Street)
			for _, token := range streetTokens {
				streetTokenFreq[token]++
			}

			mu.Lock()
			// Add name tokens to customerTokens and docFreqName
			for token, freq := range nameTokenFreq {
				customerTokens = append(customerTokens, struct {
					CustomerID int
					EntityType int
					Token      string
					Frequency  int
				}{
					CustomerID: c.ID,
					EntityType: 2, // EntityTypeID 2 for customer full name
					Token:      token,
					Frequency:  freq,
				})
				docFreqName[token]++
			}

			// Add street tokens to customerTokens and docFreqStreet
			for token, freq := range streetTokenFreq {
				customerTokens = append(customerTokens, struct {
					CustomerID int
					EntityType int
					Token      string
					Frequency  int
				}{
					CustomerID: c.ID,
					EntityType: 1, // EntityTypeID 1 for street address
					Token:      token,
					Frequency:  freq,
				})
				docFreqStreet[token]++
			}
			mu.Unlock()
			<-sem
		}(customer)
	}

	wg.Wait()

	idfName := calculateIDF(totalDocs, docFreqName)
	idfStreet := calculateIDF(totalDocs, docFreqStreet)

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "TRUNCATE TABLE tokens_idf, customer_tokens RESTART IDENTITY")
	if err != nil {
		log.Fatal(err)
	}

	insertIDF := "INSERT INTO tokens_idf (entity_type_id, ngram_token, ngram_idf) VALUES ($1, $2, $3)"
	batchSize := 1000
	idfCount := 0

	// Insert IDF values for street address tokens in batches
	for token, idfValue := range idfStreet {
		_, err := tx.Exec(ctx, insertIDF, 1, token, idfValue) // EntityTypeID 1 for street address
		if err != nil {
			log.Fatal(err)
		}
		idfCount++
		if idfCount%batchSize == 0 {
			if err := tx.Commit(ctx); err != nil {
				log.Fatal(err)
			}
			tx, err = pool.Begin(ctx)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// Insert IDF values for customer full name tokens in batches
	for token, idfValue := range idfName {
		_, err := tx.Exec(ctx, insertIDF, 2, token, idfValue) // EntityTypeID 2 for customer full name
		if err != nil {
			log.Fatal(err)
		}
		idfCount++
		if idfCount%batchSize == 0 {
			if err := tx.Commit(ctx); err != nil {
				log.Fatal(err)
			}
			tx, err = pool.Begin(ctx)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	insertCustomerTokens := "INSERT INTO customer_tokens (customer_id, entity_type_id, ngram_token, ngram_frequency) VALUES ($1, $2, $3, $4)"
	tokenCount := 0

	// Insert customer tokens in batches
	for _, ct := range customerTokens {
		_, err := tx.Exec(ctx, insertCustomerTokens, ct.CustomerID, ct.EntityType, ct.Token, ct.Frequency)
		if err != nil {
			log.Fatal(err)
		}
		tokenCount++
		if tokenCount%batchSize == 0 {
			if err := tx.Commit(ctx); err != nil {
				log.Fatal(err)
			}
			tx, err = pool.Begin(ctx)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("TF/IDF calculation and insertion completed successfully.")
}
