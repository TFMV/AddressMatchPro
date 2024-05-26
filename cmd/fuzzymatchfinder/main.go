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

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/TFMV/FuzzyMatchFinder/internal/matcher"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v2"
)

type Config struct {
	DBCreds struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	} `yaml:"db_creds"`
}

func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal config file: %v", err)
	}

	return &config, nil
}

func main() {
	// Load the configuration file
	configPath := "/Users/thomasmcgeehan/FuzzyMatchFinder/FuzzyMatchFinder/config.yaml"
	config, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fmt.Println("Config loaded successfully")

	// Create the database connection string
	databaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		config.DBCreds.Username,
		config.DBCreds.Password,
		config.DBCreds.Host,
		config.DBCreds.Port,
		config.DBCreds.Database,
	)

	// Parse the database connection string
	dbConfig, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v\n", err)
	}

	// Create the connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer pool.Close()
	fmt.Println("Database connection pool created successfully")

	// Load reference entities once
	referenceEntities := matcher.LoadReferenceEntities(pool)
	fmt.Println("Reference entities loaded successfully")

	// Process customer addresses and generate binary keys with concurrency
	matcher.ProcessCustomerAddresses(pool, referenceEntities, 10)
	fmt.Println("Customer addresses processed and binary keys generated successfully")

	// Generate TF/IDF vectors
	matcher.GenerateTFIDF(pool)
	fmt.Println("TF/IDF vectors generated successfully")

	// Call the Python script to generate embeddings
	cmd := exec.Command("python", "../python-ml/generate_embeddings.py")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to run Python script: %v\nOutput: %s", err, output)
	}
	fmt.Println("Python script executed successfully")
	fmt.Println(string(output))
}
