.PHONY: build build-server run run-server clean fmt vet test test-unit test-e2e bench coverage test-race version get set del compact

# Binary name
BINARY=logra


# Build the binary
build:
	go build -o $(BINARY) ./cmd/logra

# Run without building binary
run:
	go run ./cmd/logra $(CMD) $(ARGS)

# Build the server binary
build-server:
	go build -o logra-server ./cmd/logra-server

# Run server without building binary
run-server:
	go run ./cmd/logra-server -addr $(or $(ADDR),:6379) -db $(or $(DB),logra_data)

# Clean build artifacts
clean:
	rm -f $(BINARY) logra-server
	go clean

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run all tests
test:
	go test -v ./...

# Run unit tests only (skip E2E tests)
test-unit:
	go test -v -short ./...

# Run E2E tests only
test-e2e:
	go test -v -run "^TestE2E" ./...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Generate coverage report
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with race detection
test-race:
	go test -race ./...

# Quick commands
version: build
	./$(BINARY) version

get: build
	@if [ -z "$(KEY)" ]; then echo "Usage: make get KEY=mykey"; exit 1; fi
	./$(BINARY) get $(KEY)

set: build
	@if [ -z "$(KEY)" ] || [ -z "$(VALUE)" ]; then echo "Usage: make set KEY=mykey VALUE=myvalue"; exit 1; fi
	./$(BINARY) set $(KEY) $(VALUE)

del: build
	@if [ -z "$(KEY)" ]; then echo "Usage: make del KEY=mykey"; exit 1; fi
	./$(BINARY) del $(KEY)

compact: build
	./$(BINARY) compact

# Help
help:
	@echo "Available targets:"
	@echo "  build      - Build the binary"
	@echo "  run        - Run with CMD and ARGS (e.g., make run CMD=version)"
	@echo "  clean      - Remove build artifacts"
	@echo "  fmt        - Format Go code"
	@echo "  vet        - Run go vet"
	@echo "  test       - Run all tests"
	@echo "  test-unit  - Run unit tests only (skip E2E)"
	@echo "  test-e2e   - Run E2E tests only"
	@echo "  bench      - Run benchmarks"
	@echo "  coverage   - Generate coverage report (coverage.html)"
	@echo "  test-race  - Run tests with race detection"
	@echo "  version    - Build and show version"
	@echo "  get        - Get a key (e.g., make get KEY=mykey)"
	@echo "  set        - Set a key-value (e.g., make set KEY=mykey VALUE=myvalue)"
	@echo "  del        - Delete a key (e.g., make del KEY=mykey)"
	@echo "  compact    - Run database compaction"
	@echo "  build-server - Build the server binary"
	@echo "  run-server - Start RESP server (e.g., make run-server ADDR=:6379 DB=logra_data)"
	@echo "  help       - Show this help"
