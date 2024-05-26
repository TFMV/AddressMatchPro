package matcher

import (
	"context"
	"log"
	"math"
	"strings"
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
	rows, err := pool.Query(context.Background(), "SELECT id, lower(first_name) || ' ' || lower(last_name) as name, lower(street) as street FROM customers")
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
	docFreq := make(map[string]int)
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

			tokenFreq := make(map[string]int)

			// Tokenize and count tokens for name
			nameTokens := tokenize(c.Name)
			for _, token := range nameTokens {
				tokenFreq[token]++
			}

			// Tokenize and count tokens for street
			streetTokens := tokenize(c.Street)
			for _, token := range streetTokens {
				tokenFreq[token]++
			}

			mu.Lock()
			for token, freq := range tokenFreq {
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
				docFreq[token]++
			}
			mu.Unlock()
			<-sem
		}(customer)
	}

	wg.Wait()

	idf := calculateIDF(totalDocs, docFreq)

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
	idfBatch := make([]string, 0, batchSize)
	args := make([]interface{}, 0, batchSize*3)

	for token, idfValue := range idf {
		idfBatch = append(idfBatch, insertIDF)
		args = append(args, 2, token, idfValue)

		if len(idfBatch) == batchSize {
			_, err := tx.Exec(ctx, strings.Join(idfBatch, "; "), args...)
			if err != nil {
				log.Fatal(err)
			}
			idfBatch = idfBatch[:0]
			args = args[:0]
		}
	}

	if len(idfBatch) > 0 {
		_, err := tx.Exec(ctx, strings.Join(idfBatch, "; "), args...)
		if err != nil {
			log.Fatal(err)
		}
	}

	insertCustomerTokens := "INSERT INTO customer_tokens (customer_id, entity_type_id, ngram_token, ngram_frequency) VALUES ($1, $2, $3, $4)"
	tokenBatch := make([]string, 0, batchSize)
	tokenArgs := make([]interface{}, 0, batchSize*4)

	for _, ct := range customerTokens {
		tokenBatch = append(tokenBatch, insertCustomerTokens)
		tokenArgs = append(tokenArgs, ct.CustomerID, ct.EntityType, ct.Token, ct.Frequency)

		if len(tokenBatch) == batchSize {
			_, err := tx.Exec(ctx, strings.Join(tokenBatch, "; "), tokenArgs...)
			if err != nil {
				log.Fatal(err)
			}
			tokenBatch = tokenBatch[:0]
			tokenArgs = tokenArgs[:0]
		}
	}

	if len(tokenBatch) > 0 {
		_, err := tx.Exec(ctx, strings.Join(tokenBatch, "; "), tokenArgs...)
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("TF/IDF calculation and insertion completed successfully.")
}
