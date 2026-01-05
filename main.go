package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type LograDB struct {
	keyDict          map[string]bool
	db_file_path     string
	version          string
	config_file_path string
}

func (db *LograDB) GetVersion() string {
	return db.version
}

func (db *LograDB) populateAllKeys(file *os.File) (bool, error) {
	bufferedReader := bufio.NewReader(file)
	for {
		line, _, err := bufferedReader.ReadLine()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return false, err
		}

		splittedLine := strings.Split(string(line), ":")
		key := splittedLine[0]
		db.keyDict[key] = true
	}
	return true, nil
}

func (db *LograDB) HasKey(key string) bool {
	_, exists := db.keyDict[key]
	return exists
}

func (db *LograDB) writeKeyPair(key string, val string) error {

	db_file, err := os.OpenFile(db.db_file_path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer db_file.Close()

	_, err = db_file.WriteString(fmt.Sprintf("%s:%s\n", key, val))
	if err != nil {
		return err
	}
	db.keyDict[key] = true
	return nil

}

func NewLograDB(db_file_path string, version string, config_file_path string) (*LograDB, error) {
	var logra_db *LograDB
	logra_db = &LograDB{
		keyDict:          make(map[string]bool),
		db_file_path:     db_file_path,
		version:          version,
		config_file_path: config_file_path,
	}

	db_file, err := os.Open(db_file_path)

	if err != nil {
		fmt.Println("Error opening database file:", err)
		return nil, err
	}
	defer db_file.Close()

	populated, err := logra_db.populateAllKeys(db_file)
	if populated {
		fmt.Println("All keys populated successfully.")
	} else {
		fmt.Println("Failed to populate keys:", err)
		return nil, err
	}

	if err != nil {
		fmt.Println("Error opening database file:", err)
		return nil, err
	}
	return logra_db, nil

}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <db_file_path> <config_file_path>")
		return
	}
	db_file_path := os.Args[1]
	version := "1.0.0"
	config_file_path := os.Args[2]
	command := os.Args[3]
	lograDB, err := NewLograDB(db_file_path, version, config_file_path)
	if err != nil {
		fmt.Println("Failed to initialize LograDB:", err)
		return
	}
	switch command {
	case "version":
		fmt.Println("LograDB Version:", lograDB.GetVersion())
		return
	case "haskey":
		if len(os.Args) < 5 {
			fmt.Println("Usage: go run main.go <db_file_path> <config_file_path> haskey <key>")
			return
		}
		key := os.Args[4]
		if lograDB.HasKey(key) {
			fmt.Printf("Key '%s' exists in the database.\n", key)
		} else {
			fmt.Printf("Key '%s' does not exist in the database.\n", key)
		}
		return
	case "writekey":
		if len(os.Args) < 6 {
			fmt.Println("Usage: go run main.go <db_file_path> <config_file_path> writekey <key> <value>")
			return
		}
		key := os.Args[4]
		value := os.Args[5]
		err := lograDB.writeKeyPair(key, value)
		if err != nil {
			fmt.Println("Failed to write key-value pair:", err)
		}
		return
	default:
		fmt.Println("Unknown command. Available commands: version, haskey, writekey")
		return

	}

	fmt.Println("LograDB initialized with the following parameters:")
	fmt.Printf("DB File Path: %s\n", lograDB.db_file_path)
	fmt.Printf("Version: %s\n", lograDB.version)
	fmt.Printf("Config File Path: %s\n", lograDB.config_file_path)

}
