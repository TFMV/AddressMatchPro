package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DBCreds struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

// NewConnection creates a new database connection pool
func NewConnection(creds DBCreds) (*pgxpool.Pool, error) {
	databaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		creds.Username,
		creds.Password,
		creds.Host,
		creds.Port,
		creds.Database,
	)

	config, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %v", err)
	}

	return pool, nil
}
