package main

import (
	"context"
	"log"
	"os"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
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
	referenceEntities := matcher.LoadReferenceEntities(pool)

	// Process customer addresses and generate binary keys with concurrency
	matcher.ProcessCustomerAddresses(pool, referenceEntities, 10)
}
