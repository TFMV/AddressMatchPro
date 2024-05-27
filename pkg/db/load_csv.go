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
// The above copyright notice shall be included in all
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

package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CsvSource implements the pgx.CopyFromSource interface
type CsvSource struct {
	reader *csv.Reader
	cols   []string
}

func (s *CsvSource) Next() bool {
	record, err := s.reader.Read()
	if err != nil {
		return false
	}
	s.cols = record
	return true
}

func (s *CsvSource) Values() ([]interface{}, error) {
	values := make([]interface{}, len(s.cols))
	for i, col := range s.cols {
		values[i] = col
	}
	return values, nil
}

func (s *CsvSource) Err() error {
	return nil
}

func main() {
	start := time.Now()

	// Load the configuration
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/Users/thomasmcgeehan/FuzzyMatchFinder/FuzzyMatchFinder/config.yaml"
	}
	config, err := matcher.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create the database connection string
	databaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		config.DBCreds.Username,
		config.DBCreds.Password,
		config.DBCreds.Host,
		config.DBCreds.Port,
		config.DBCreds.Database,
	)

	// Create the connection pool
	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatalf("Unable to acquire a connection: %v\n", err)
	}
	defer conn.Release()

	// Get the CSV file path from command-line arguments
	csvFilePath := flag.String("csv", "", "Path to the CSV file")
	flag.Parse()

	if *csvFilePath == "" {
		log.Fatalf("CSV file path is required")
	}

	// Open the CSV file
	file, err := os.Open(*csvFilePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	headers, err := reader.Read() // Read the header
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	csvSource := &CsvSource{reader: reader}

	copyCount, err := conn.Conn().CopyFrom(
		context.Background(),
		pgx.Identifier{config.DBCreds.LoadTable},
		headers,
		csvSource,
	)

	if err != nil {
		log.Fatalf("Error copying data to database: %v", err)
		os.Exit(1)
	}

	fmt.Printf("Copied %v rows to %s table in %v\n", copyCount, config.DBCreds.LoadTable, time.Since(start))
}
