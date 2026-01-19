# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Logra is a simple key-value database written in Go. It stores data in a flat file format with key:value pairs (one per line) and maintains an in-memory index (map) of keys for fast lookups.

## Build and Run Commands

**Run the application:**
```bash
go run main.go <db_file_path> <config_file_path> <command> [args...]
```

**Example commands:**
```bash
# Check version
go run main.go logra.db config.txt version

# Check if key exists
go run main.go logra.db config.txt get <key>

# Set a key-value pair
go run main.go logra.db config.txt set <key> <value>
```

**Build executable:**
```bash
go build -o logra main.go
```

## Architecture

### Core Components

**LograDB struct** (main.go:10-15): The main database type containing:
- `keyDict`: In-memory map[string]int that stores all keys for O(1) lookup
- `db_file_path`: Path to the database file
- `version`: Database version string
- `config_file_path`: Path to config file (currently unused in logic)

### Data Flow

1. **Initialization** (NewLograDB): Opens the database file, reads all lines, and populates the in-memory `keyDict` with all keys found
2. **Key lookups** (HasKey): Checks existence using the in-memory map only
3. **Writes** (writeKeyPair): Appends new key:value to file and updates in-memory map

### File Format

The database file uses a simple line-based format:
```
key:value
```

Each line contains one key-value pair separated by a colon.

## Important Implementation Details

- The database does NOT support updating existing keys - it only checks if a key exists or appends new keys
- The `keyDict` map values are always set to 1 (presence indicator only)
- All keys are loaded into memory on startup by reading the entire database file
- No actual value retrieval is implemented - only key existence checking
- The config file path is accepted but not used in current implementation
