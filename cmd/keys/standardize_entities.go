package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/TFMV/FuzzyMatchFinder/pkg/pca"
	"github.com/TFMV/FuzzyMatchFinder/pkg/tfidf"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"gonum.org/v1/gonum/mat"
)

// StandardizeAddress concatenates and standardizes address fields
func standardizeAddress(firstName, lastName, street, city, state, zipCode string) string {
	address := fmt.Sprintf("%s %s %s %s %s %s", firstName, lastName, street, city, state, zipCode)
	return strings.ToUpper(strings.TrimSpace(address))
}

func main() {
	// Database connection URL
	dbURL := "postgresql://postgres:password@localhost:5432/tfmv"

	// Connect to the PostgreSQL database
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatalf("Unable to parse database URL: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	// Query the customer data with 10% sampling
	query := "SELECT first_name, last_name, street, city, state, zip_code FROM customers TABLESAMPLE SYSTEM (10)"
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var entities []string
	for rows.Next() {
		var firstName, lastName, street, city, state, zipCode pgtype.Text
		if err := rows.Scan(&firstName, &lastName, &street, &city, &state, &zipCode); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		entity := standardizeAddress(firstName.String, lastName.String, street.String, city.String, state.String, zipCode.String)
		entities = append(entities, entity)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Failed to read rows: %v", err)
	}

	// Process the sampled data
	processBatch(pool, entities)
}

func processBatch(pool *pgxpool.Pool, entities []string) {
	// Convert text data to TF-IDF vectors using custom vectorizer
	vectorizer := tfidf.NewVectorizer()
	X := vectorizer.FitTransform(entities)

	// Convert to Dense matrix
	numRows, numCols := len(X), len(X[0])
	matData := make([]float64, numRows*numCols)
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			matData[i*numCols+j] = X[i][j]
		}
	}
	mat := mat.NewDense(numRows, numCols, matData)

	// Perform PCA
	pcaModel := pca.NewPCA(10)
	X_pca := pcaModel.FitTransform(mat)

	// Find the index of the representative entities
	scores := X_pca.RawMatrix().Data
	var representativeIndices []int
	for i := range scores {
		representativeIndices = append(representativeIndices, i)
	}

	var representativeEntities []string
	for _, idx := range representativeIndices[:10] {
		representativeEntities = append(representativeEntities, entities[idx])
	}

	// Insert representative entities into the reference_entities table
	batch := &pgx.Batch{}
	for _, entity := range representativeEntities {
		batch.Queue("INSERT INTO reference_entities (entity_value) VALUES ($1)", entity)
	}

	results := pool.SendBatch(context.Background(), batch)
	if err := results.Close(); err != nil {
		log.Fatalf("Failed to execute batch insert: %v", err)
	}

	fmt.Println("Inserted representative entities into the reference_entities table.")
}
