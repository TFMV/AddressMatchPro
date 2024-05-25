package matcher

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Connect to the database
	databaseUrl := os.Getenv("DATABASE_URL")
	config, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v\n", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer pool.Close()

	// Read reference addresses once
	referenceEntities := loadReferenceEntities(pool)

	// Read customer addresses from the database
	rows, err := pool.Query(context.Background(), "SELECT id, street FROM customers")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	// Process each customer with concurrency
	var wg sync.WaitGroup
	addressCh := make(chan [2]interface{}, 1000)
	resultCh := make(chan [2]interface{}, 1000)

	// Start worker goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range addressCh {
				id := addr[0].(int)
				street := addr[1].(string)
				binaryKey := calculateBinaryKey(referenceEntities, street)
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
				insertBatch(pool, batch)
				batch = batch[:0] // reset batch
			}
		}
		if len(batch) > 0 {
			insertBatch(pool, batch)
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

// Load reference entities into memory
func loadReferenceEntities(pool *pgxpool.Pool) []string {
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
		referenceEntities = append(referenceEntities, strings.ToLower(entityValue))
	}
	return referenceEntities
}

// Insert a batch of results into the database
func insertBatch(pool *pgxpool.Pool, batch [][2]interface{}) {
	batchSize := len(batch)
	ids := make([]int, batchSize)
	keys := make([]string, batchSize)

	for i, record := range batch {
		ids[i] = record[0].(int)
		keys[i] = record[1].(string)
	}

	_, err := pool.Exec(context.Background(),
		"INSERT INTO customer_keys (customer_id, binary_key) SELECT UNNEST($1::int[]), UNNEST($2::text[])",
		ids, keys,
	)
	if err != nil {
		log.Fatalf("Batch insert failed: %v\n", err)
	}
}

// Calculate the binary key for a given street address
func calculateBinaryKey(referenceEntities []string, street string) string {
	street = strings.ToLower(street)
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
