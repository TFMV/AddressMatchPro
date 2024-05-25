package matcher

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/TFMV/FuzzyMatchFinder/internal/standardizer"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Load reference entities into memory
func LoadReferenceEntities(pool *pgxpool.Pool) []string {
	rows, err := pool.Query(context.Background(), "SELECT entity_value FROM reference_entities")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	var referenceEntities []string
	for rows.Next() {
		var entityValue string
		err := rows.Scan(&entityValue)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		referenceEntities = append(referenceEntities, entityValue)
	}
	return referenceEntities
}

// Calculate the binary key for a given street address
func CalculateBinaryKey(referenceEntities []string, street string) string {
	var binaryKey strings.Builder
	n := 3

	for _, referenceStreet := range referenceEntities {
		similarity := ngramFrequencySimilarity(street, referenceStreet, n)
		if similarity >= 0.7 {
			binaryKey.WriteString("1")
		} else {
			binaryKey.WriteString("0")
		}
		if binaryKey.Len() >= 10 {
			break
		}
	}

	// Ensure the binary key is exactly 10 characters long
	for binaryKey.Len() < 10 {
		binaryKey.WriteString("0")
	}

	return binaryKey.String()
}

// Insert a batch of results into the database
func InsertBatch(pool *pgxpool.Pool, batch [][2]interface{}) {
	batchSize := len(batch)
	ids := make([]interface{}, batchSize)
	keys := make([]interface{}, batchSize)

	for i, record := range batch {
		ids[i] = record[0]
		keys[i] = record[1]
	}

	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_keys (customer_id, binary_key) SELECT UNNEST($1::int[]), UNNEST($2::text[])",
		ids, keys,
	)
	if err != nil {
		log.Fatalf("Batch insert failed: %v\n", err)
	}
}

// Process customer addresses and generate binary keys
func ProcessCustomerAddresses(pool *pgxpool.Pool, referenceEntities []string, numWorkers int) {
	rows, err := pool.Query(context.Background(), "SELECT id, street FROM customers")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	var wg sync.WaitGroup
	addressCh := make(chan [2]interface{}, 1000)
	resultCh := make(chan [2]interface{}, 1000)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addressCh {
				id := addr[0].(int)
				street := addr[1].(string)
				standardizedStreet, err := standardizer.StandardizeAddress("", "", street, "", "", "")
				if err != nil {
					log.Printf("Failed to standardize address: %v\n", err)
					continue
				}
				binaryKey := CalculateBinaryKey(referenceEntities, strings.ToLower(standardizedStreet))
				resultCh <- [2]interface{}{id, binaryKey}
			}
		}()
	}

	// Insert results in batches
	go func() {
		var batchSize = 1000
		var batch [][2]interface{}
		for res := range resultCh {
			batch = append(batch, res)
			if len(batch) >= batchSize {
				InsertBatch(pool, batch)
				batch = batch[:0] // reset batch
			}
		}
		if len(batch) > 0 {
			InsertBatch(pool, batch)
		}
	}()

	// Enqueue addresses for processing
	for rows.Next() {
		var id int
		var street string
		err := rows.Scan(&id, &street)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		addressCh <- [2]interface{}{id, street}
	}
	close(addressCh)
	wg.Wait()
	close(resultCh)
}
