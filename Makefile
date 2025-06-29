# Makefile for the litestore project

.PHONY: help all test fmt tidy lint clean

# Default target executed when you run `make`
all: test

help:
	@echo "Makefile for the litestore project"
	@echo ""
	@echo "Usage:"
	@echo "    make <target>"
	@echo ""
	@echo "Targets:"
	@echo "    help      Show this help message"
	@echo "    test      Run all tests with race detector"
	@echo "    fmt       Format all go files"
	@echo "    tidy      Tidy go modules"
	@echo "    lint      Run golangci-lint linter"
	@echo "    clean     Clean up test cache"
	@echo ""

test:
	@echo "Running tests..."
	go test -v -race ./...

test-cover:
	@echo "Running tests..."
	go test -v -race -coverprofile cover.out ./...
	go tool cover -html cover.out -o cover.html
	open cover.html

fmt:
	@echo "Formatting code..."
	go fmt ./...

tidy:
	@echo "Tidying modules..."
	go mod tidy

lint:
	@echo "Linting code..."
	@# Install golangci-lint with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "golangci-lint could not be found. Please install it first."; \
		echo "go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi
	golangci-lint run

clean:
	@echo "Cleaning up..."
	go clean -testcache

