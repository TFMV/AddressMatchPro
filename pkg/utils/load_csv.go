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

package utils

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/TFMV/AddressMatchPro/internal/matcher"
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

// LoadCSV loads the CSV file into the specified table in the database
func LoadCSV(pool *pgxpool.Pool, csvFilePath string, runID int) error {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read() // Read the header
	if err != nil {
		return fmt.Errorf("error reading CSV header: %w", err)
	}

	csvSource := &CsvSource{reader: reader}

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("unable to acquire a connection: %w", err)
	}
	defer conn.Release()

	// Load the configuration
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/path/to/your/config.yaml"
	}
	config, err := matcher.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
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
	pool, err = pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	defer pool.Close()

	tx, err := conn.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	copyCount, err := conn.Conn().CopyFrom(
		context.Background(),
		pgx.Identifier{config.DBCreds.LoadTable},
		headers,
		csvSource,
	)
	if err != nil {
		tx.Rollback(context.Background())
		return fmt.Errorf("error copying data to database: %w", err)
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	log.Printf("Copied %v rows to %s table", copyCount, config.DBCreds.LoadTable)
	return nil
}
