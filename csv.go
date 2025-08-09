package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
)

// Layer 1 - Atomic functions
// createCSVWriter creates a new CSV file and returns the writer and file
func createCSVWriter(filename string) (*csv.Writer, *os.File, error) {
	// Create the file
	file, err := os.Create(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s: %w", filename, err)
	}

	// Create CSV writer that writes to the file
	writer := csv.NewWriter(file)

	return writer, file, nil
}

// writeHeaders writes column names as the first row of CSV
func writeHeaders(writer *csv.Writer, columns []string) error {
	err := writer.Write(columns)
	if err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}
	return nil
}

// getColumns extracts column names from sql.Rows
func getColumns(rows *sql.Rows) ([]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}
	return columns, nil
}

// writeDataRows writes rows to the file.
// No return and the output is written to file.
func writeDataRows(writer *csv.Writer, rows *sql.Rows, columns []string) error {
	// rows are given from snowflake
	// Loop through rows
	for rows.Next() {
		// variable for each column
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		// we need pointers to modify what they are pointing to (original values)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		//rows being scanned into a variables
		err := rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("couldn't scan rows: %w", err)
		}
		// converting to strings so CSV could be created
		stringRow := make([]string, len(columns))
		for i, val := range values {
			if val != nil {
				stringRow[i] = fmt.Sprintf("%v", val)
			} else {
				stringRow[i] = ""
			}
		}
		err = writer.Write(stringRow)
		if err != nil {
			return fmt.Errorf("failed to write rows: %w", err)
		}

	}
	return nil
}

// Layer 2 - Orchestration
func convertToCSV(rows *sql.Rows, filename string) error {
	// Step 1: Get column names
	columns, err := getColumns(rows)
	if err != nil {
		return err
	}

	// Step 2: Create CSV writer
	writer, file, err := createCSVWriter(filename)
	if err != nil {
		return err
	}
	defer file.Close()   // Ensure file gets closed
	defer writer.Flush() // Ensure data gets written

	// Step 3: Write headers
	err = writeHeaders(writer, columns)
	if err != nil {
		return err
	}

	// Step 4: Write rows
	err = writeDataRows(writer, rows, columns)
	if err != nil {
		return err
	}

	fmt.Printf("Created CSV file %s with headers: %v\n", filename, columns)

	return nil
}
