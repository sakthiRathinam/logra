package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type LograDB struct {
	keyDict    map[string]int64
	dbFilePath string
	version    string
	activeFile *os.File
}

func (db *LograDB) GetVersion() string {
	return db.version
}

func (db *LograDB) populateAllKeys() (bool, error) {
	bufferedReader := bufio.NewReader(db.activeFile)
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
		db.keyDict[key] = 1
	}
	return true, nil
}

func (db *LograDB) HasKey(key string) bool {
	_, exists := db.keyDict[key]
	return exists
}

func (db *LograDB) GetValue(key string) (string, error) {
	offset, exists := db.keyDict[key]
	if !exists {
		return "", fmt.Errorf("key not found")
	}

	_, err := db.activeFile.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	bufferedReader := bufio.NewReader(db.activeFile)
	line, _, err := bufferedReader.ReadLine()
	if err != nil {
		return "", err
	}

	splittedLine := strings.Split(string(line), ":")
	if len(splittedLine) < 2 {
		return "", fmt.Errorf("invalid data format")
	}
	return splittedLine[1], nil
}
func (db *LograDB) writeKeyPair(key string, val string) error {

	writer := bufio.NewWriter(db.activeFile)

	offset, err := db.activeFile.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	_, err = writer.WriteString(fmt.Sprintf("%s:%s\n", key, val))
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	db.keyDict[key] = offset
	return nil

}

func NewLograDB(db_file_path string, version string) (*LograDB, error) {
	var logra_db *LograDB

	dbFile, err := os.Open(db_file_path)
	if err != nil {
		fmt.Println("Error creating/opening database file:", err)
		return nil, err
	}
	logra_db = &LograDB{
		keyDict:    make(map[string]int64),
		dbFilePath: db_file_path,
		version:    version,
		activeFile: dbFile,
	}

	db_file, err := os.Open(db_file_path)

	if err != nil {
		fmt.Println("Error opening database file:", err)
		return nil, err
	}
	defer db_file.Close()

	populated, err := logra_db.populateAllKeys()
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
	command := os.Args[2]

	fmt.Println("version command:", version)
	fmt.Println("db_file_path command:", db_file_path)
	fmt.Println("command:", command)

	lograDB, err := NewLograDB(db_file_path, version)
	if err != nil {
		fmt.Println("Failed to initialize LograDB:", err)
		return
	}
	switch command {
	case "version":
		fmt.Println("LograDB Version:", lograDB.GetVersion())
		return
	case "get":
		if len(os.Args) < 3 {
			fmt.Println("Usage: go run main.go <db_file_path> get <key>")
			return
		}
		key := os.Args[3]
		if lograDB.HasKey(key) {
			fmt.Printf("Key '%s' exists in the database.\n", key)
		} else {
			fmt.Printf("Key '%s' does not exist in the database.\n", key)
		}
		return
	case "set":
		if len(os.Args) < 4 {
			fmt.Println("Usage: go run main.go <db_file_path> set <key> <value>")
			return
		}
		key := os.Args[3]
		value := os.Args[4]
		err := lograDB.writeKeyPair(key, value)
		if err != nil {
			fmt.Println("Failed to write key-value pair:", err)
		}
		return
	default:
		fmt.Println("Unknown command. Available commands: version, get, set")
		return

	}

}
