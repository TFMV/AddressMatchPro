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
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Connect to the database
	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		log.Fatalf("DATABASE_URL environment variable is not set")
	}

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
	referenceEntities := LoadReferenceEntities(pool)
	fmt.Println("Reference entities loaded successfully")

	// Clear old candidates with run_id = 0
	clearOldCandidates(pool)
	fmt.Println("Old candidates cleared successfully")

	// Process customer addresses and generate binary keys with concurrency
	runID := 0
	ProcessCustomerAddresses(pool, referenceEntities, 10, runID)
	fmt.Println("Customer addresses processed and binary keys generated successfully")
}

func clearOldCandidates(pool *pgxpool.Pool) {
	tables := []string{
		"customer_keys",
		"customer_tokens",
		"tokens_idf",
		"customer_vector_embedding",
	}
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s WHERE run_id = 0", table)
		if _, err := pool.Exec(context.Background(), query); err != nil {
			log.Fatalf("Failed to clear old candidates from %s: %v", table, err)
		}
	}
}
