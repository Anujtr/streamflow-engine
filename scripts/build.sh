#!/bin/bash

set -e

echo "Building StreamFlow Engine..."

# Clean previous builds
rm -rf bin/

# Create bin directory
mkdir -p bin/

# Build server
echo "Building server..."
go build -o bin/streamflow ./cmd/streamflow

# Build client example
echo "Building example client..."
go build -o bin/example ./cmd/example

# Build benchmark tool
echo "Building benchmark tool..."
go build -o bin/benchmark ./cmd/benchmark

echo "Build complete! Binaries available in bin/"
echo ""
echo "To run the server: ./bin/streamflow"
echo "To run the example: ./bin/example"
echo "To run benchmarks: ./bin/benchmark"