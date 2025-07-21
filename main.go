package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/snowflakedb/gosnowflake"
)

func main() {
	fmt.Println("Testing Snowflake connection...")

	// Load .env file (only in development)
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file:", err)
	}

	// Test connection
	db, err := connectToSnowflake()
	if err != nil {
		log.Fatal("Failed to connect to Snowflake:", err)
	}
	defer db.Close()

	fmt.Println("✓ Snowflake connection successful!")

	// Test simple query
	fmt.Println("Testing simple query...")
	err = testSimpleQuery(db)
	if err != nil {
		log.Fatal("Simple query failed:", err)
	}

	// Test sample data query
	fmt.Println("Testing sample data query...")
	err = testSampleDataQuery(db)
	if err != nil {
		log.Fatal("Sample data query failed:", err)
	}

	fmt.Println("🎉 All tests passed! Ready for Phase 2.")
}

// connectToSnowflake establishes connection to Snowflake
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

// testSimpleQuery runs a basic test
func testSimpleQuery(db *sql.DB) error {
	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to execute SELECT 1: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected result: got %d, expected 1", result)
	}

	fmt.Println("✓ SELECT 1 test passed")
	return nil
}

// testSampleDataQuery tests access to sample data
func testSampleDataQuery(db *sql.DB) error {
	query := "SELECT N_NATIONKEY, N_NAME FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION LIMIT 3"

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute sample query: %w", err)
	}
	defer rows.Close()

	fmt.Println("Sample data from NATION table:")
	count := 0
	for rows.Next() {
		var nationKey int
		var nationName string

		err := rows.Scan(&nationKey, &nationName)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		fmt.Printf("  %d: %s\n", nationKey, nationName)
		count++
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	fmt.Printf("✓ Successfully retrieved %d rows from sample data\n", count)
	return nil
}
