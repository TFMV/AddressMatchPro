package main

import (
	"fmt"
	"log"

	"github.com/TFMV/FuzzyMatchFinder/pkg/api"
	"github.com/TFMV/FuzzyMatchFinder/pkg/config"
	"github.com/TFMV/FuzzyMatchFinder/pkg/db"
	"github.com/gin-gonic/gin"
)

func main() {
	// Load configuration
	configPath := "config.yaml"
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fmt.Println("Config loaded successfully")

	// Create the database connection pool
	pool, err := db.NewConnection(db.DBCreds{
		Host:     cfg.DBCreds.Host,
		Port:     cfg.DBCreds.Port,
		Username: cfg.DBCreds.Username,
		Password: cfg.DBCreds.Password,
		Database: cfg.DBCreds.Database,
	})
	if err != nil {
		log.Fatalf("Failed to create database connection pool: %v", err)
	}
	defer pool.Close()
	fmt.Println("Database connection pool created successfully")

	// Set up the HTTP server
	router := gin.Default()
	api.SetupRoutes(router, pool)
	fmt.Println("Starting server on :8080")
	log.Fatal(router.Run(":8080"))
}
