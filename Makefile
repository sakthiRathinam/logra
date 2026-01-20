.PHONY: build run clean fmt vet test version get set

# Binary name
BINARY=logra

# Default database and config files
DB_FILE=logra.db
CONFIG_FILE=config.txt

# Build the binary
build:
	go build -o $(BINARY) main.go

# Run without building binary
run:
	go run main.go $(DB_FILE) $(CONFIG_FILE) $(CMD) $(ARGS)

# Clean build artifacts
clean:
	rm -f $(BINARY)
	go clean

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run tests
test:
	go test -v ./...

# Quick commands
version: build
	./$(BINARY) $(DB_FILE) $(CONFIG_FILE) version

get: build
	@if [ -z "$(KEY)" ]; then echo "Usage: make get KEY=mykey"; exit 1; fi
	./$(BINARY) $(DB_FILE) $(CONFIG_FILE) get $(KEY)

set: build
	@if [ -z "$(KEY)" ] || [ -z "$(VALUE)" ]; then echo "Usage: make set KEY=mykey VALUE=myvalue"; exit 1; fi
	./$(BINARY) $(DB_FILE) $(CONFIG_FILE) set $(KEY) $(VALUE)

# Help
help:
	@echo "Available targets:"
	@echo "  build    - Build the binary"
	@echo "  run      - Run with CMD and ARGS (e.g., make run CMD=version)"
	@echo "  clean    - Remove build artifacts"
	@echo "  fmt      - Format Go code"
	@echo "  vet      - Run go vet"
	@echo "  test     - Run tests"
	@echo "  version  - Build and show version"
	@echo "  get      - Get a key (e.g., make get KEY=mykey)"
	@echo "  set      - Set a key-value (e.g., make set KEY=mykey VALUE=myvalue)"
	@echo "  help     - Show this help"
