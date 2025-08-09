package main

import (
	"fmt"
	"log"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	mainStart := time.Now()
	fmt.Println("Testing Snowflake connection...")

	// Load .env file (only in development)
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file:", err)
	}

	// Connect to Snowflake
	db, err := connectToSnowflake()
	if err != nil {
		log.Fatal("Failed to connect to Snowflake:", err)
	}
	defer db.Close()

	fmt.Println("✓ Snowflake connection successful!")

	fmt.Println("Querying the database...")
	queryStart := time.Now()
	rows, err := executeQuery(db, "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 65000")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()
	queryDuration := time.Since(queryStart)
	fmt.Println("Query completed in:", queryDuration)

	// Convert to CSV
	fmt.Println("Converting to CSV...")
	convertingStarts := time.Now()
	err = convertToCSV(rows, "customer_data.csv")
	if err != nil {
		log.Fatal("CSV conversion failed:", err)
	}
	convertingDuration := time.Since(convertingStarts)
	fmt.Println("✓ CSV file created: customer_data.csv")
	fmt.Println("Converting completed in:", convertingDuration)
	mainDuration := time.Since(mainStart)
	fmt.Println("Program executed in:", mainDuration)
}
