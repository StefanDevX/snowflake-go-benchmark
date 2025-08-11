package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// createS3Client creates and returns S3 client
func createS3Client() (*s3.Client, error) {
	// Load AWS configuration from environment variables
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(os.Getenv("AWS_REGION")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)
	return client, nil
}

// uploadFile uploads a local file to S3 bucket
func uploadFile(client *s3.Client, bucketName, key, filePath string) error {
	// Open local file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	// Upload to s3
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	fmt.Printf("Uploading %s to s3://%s/%s\n", filePath, bucketName, key)

	return nil
}

// Master function - follows your modular pattern
func uploadToS3(filePath, bucketName, key string) error {
	// Step 1: Create S3 client
	client, err := createS3Client()
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Step 2: Upload file
	err = uploadFile(client, bucketName, key, filePath)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	return nil
}
