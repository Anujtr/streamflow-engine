#!/bin/bash

set -e

echo "Running StreamFlow Engine tests..."

# Run all tests
echo "Running unit tests..."
go test ./... -v

echo ""
echo "Running tests with coverage..."
go test ./... -cover

echo ""
echo "Running race condition tests..."
go test ./... -race

echo ""
echo "Tests completed successfully!"