package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Boostport/address"
	"github.com/TFMV/FuzzyMatchFinder/pkg/pca"
	"github.com/TFMV/FuzzyMatchFinder/pkg/tfidf"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"gonum.org/v1/gonum/mat"
)

// StandardizeAddress uses Boostport's address package to standardize the address fields
func StandardizeAddress(name, organization, street, locality, state, postalCode string) (string, string, error) {
	addr, err := address.NewValid(
		address.WithCountry("US"),
		address.WithName(name),
		address.WithOrganization(organization),
		address.WithStreetAddress([]string{strings.TrimSpace(street)}),
		address.WithLocality(strings.TrimSpace(locality)),
		address.WithAdministrativeArea(strings.TrimSpace(state)),
		address.WithPostCode(strings.TrimSpace(postalCode)),
	)
	if err != nil {
		return "", "", fmt.Errorf("error: %v, address: %s %s %s %s %s", err, street, locality, state, postalCode, name)
	}
	streetAddress := strings.Join(addr.StreetAddress, " ")
	fullAddress := fmt.Sprintf("%s, %s, %s, %s, %s, %s", name, organization, streetAddress, locality, state, postalCode)
	return streetAddress, fullAddress, nil
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

	// Query the customer data with 2% sampling and valid fields only
	query := `
	    SELECT first_name, last_name, street, city, state, zip_code
	    FROM customers
	    WHERE state IS NOT NULL AND zip_code IS NOT NULL AND city IS NOT NULL
	    ORDER BY random()
	    LIMIT 200000
	`
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var streets, fullAddresses []string
	for rows.Next() {
		var firstName, lastName, street, city, state, zipCode pgtype.Text
		if err := rows.Scan(&firstName, &lastName, &street, &city, &state, &zipCode); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Trim spaces and check for empty fields
		firstNameStr := strings.TrimSpace(firstName.String)
		lastNameStr := strings.TrimSpace(lastName.String)
		streetStr := strings.TrimSpace(street.String)
		cityStr := strings.TrimSpace(city.String)
		stateStr := strings.TrimSpace(state.String)
		zipCodeStr := strings.TrimSpace(zipCode.String)

		if cityStr == "" || stateStr == "" || zipCodeStr == "" {
			log.Printf("Skipping address due to missing fields: %s, %s, %s, %s, %s", streetStr, cityStr, stateStr, zipCodeStr, firstNameStr)
			continue
		}

		streetAddress, fullAddress, err := StandardizeAddress(
			fmt.Sprintf("%s %s", firstNameStr, lastNameStr),
			"",
			streetStr,
			cityStr,
			stateStr,
			zipCodeStr,
		)
		if err != nil {
			log.Printf("Failed to standardize address for %s %s: %v", firstNameStr, lastNameStr, err)
			continue
		}
		streets = append(streets, streetAddress)
		fullAddresses = append(fullAddresses, fullAddress)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Failed to read rows: %v", err)
	}

	// Process the sampled data
	processBatch(pool, streets, fullAddresses)
}

func processBatch(pool *pgxpool.Pool, streets, fullAddresses []string) {
	// Convert street address data to TF-IDF vectors using custom vectorizer
	vectorizer := tfidf.NewVectorizer()
	X := vectorizer.FitTransform(streets)

	// Convert to Dense matrix
	numRows, numCols := len(X), len(X[0])
	matData := make([]float64, numRows*numCols)
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			matData[i*numCols+j] = X[i][j]
		}
	}
	mat := mat.NewDense(numRows, numCols, matData)

	// Perform PCA on the street address vectors
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
		representativeEntities = append(representativeEntities, fullAddresses[idx])
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
