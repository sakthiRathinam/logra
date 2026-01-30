package main

import (
	"fmt"
	"os"

	"sakthirathinam/logra"
	"sakthirathinam/logra/internal/compact"
)

const dbDirectoryPath = "logra_data"

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: logra <command> [args...]")
		os.Exit(1)
	}

	command := os.Args[1]

	db, err := logra.Open(dbDirectoryPath, "1.0.0")
	if err != nil {
		fmt.Println("Failed to open database:", err)
		os.Exit(1)
	}
	defer db.Close()

	switch command {
	case "version":
		fmt.Println("LograDB Version:", db.Version())

	case "get":
		if len(os.Args) < 3 {
			fmt.Println("Usage: logra get <key>")
			os.Exit(1)
		}
		key := os.Args[2]

		if !db.Has(key) {
			fmt.Printf("Key '%s' does not exist.\n", key)
			os.Exit(1)
		}

		record, err := db.Get(key)
		if err != nil {
			fmt.Println("Error retrieving value:", err)
			os.Exit(1)
		}
		fmt.Printf("Key: %s\nValue: %s\n", record.Key, record.Value)

	case "set":
		if len(os.Args) < 4 {
			fmt.Println("Usage: logra set <key> <value>")
			os.Exit(1)
		}
		key := os.Args[2]
		value := os.Args[3]

		if err := db.Set(key, value); err != nil {
			fmt.Println("Failed to set key-value:", err)
			os.Exit(1)
		}
		fmt.Printf("Set '%s' = '%s'\n", key, value)

	case "del":
		if len(os.Args) < 3 {
			fmt.Println("Usage: logra del <key>")
			os.Exit(1)
		}
		key := os.Args[2]

		if err := db.Delete(key); err != nil {
			fmt.Println("Failed to delete key:", err)
			os.Exit(1)
		}
		fmt.Printf("Deleted key '%s'\n", key)

	case "compact":
		compact := compact.NewCompact(db)
		if err := compact.Execute(); err != nil {
			fmt.Println("Failed to compact database:", err)
			os.Exit(1)
		}
		fmt.Println("Compaction completed")

	default:
		fmt.Println("Unknown command. Available: version, get, set")
		os.Exit(1)
	}
}
