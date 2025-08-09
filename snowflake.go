package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/snowflakedb/gosnowflake"
)

// Establishes connection to Snowflake
func connectToSnowflake() (*sql.DB, error) {
	// Build connection string
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s",
		os.Getenv("SNOWFLAKE_USER"),
		os.Getenv("SNOWFLAKE_PASSWORD"),
		os.Getenv("SNOWFLAKE_ACCOUNT"),
		os.Getenv("SNOWFLAKE_DATABASE"),
		os.Getenv("SNOWFLAKE_SCHEMA"),
	)

	// Open connection
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// Main Query function definition
func executeQuery(db *sql.DB, query string) (*sql.Rows, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return rows, nil
}
