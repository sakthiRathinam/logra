package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type LograDB struct {
	keyDict    map[string]DBRow
	dbFilePath string
	version    string
	activeFile *os.File
}
type RecordHeader struct {
	crc       uint32
	timestamp int64
	keySize   uint32
	valueSize uint32
}
type DBRow struct {
	offset int64
	header RecordHeader
	key    string
	value  string
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
		offset, err := strconv.Atoi(splittedLine[1])
		if err != nil {
			return false, err

		}
		db.keyDict[key] = int64(offset)
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
	return splittedLine[2], nil
}

func createDBRow(key string, value string) DBRow {
	crc := uint32(0)      // Placeholder for CRC calculation
	timestamp := int64(0) // Placeholder for timestamp
	return DBRow{
		header: RecordHeader{
			crc:       crc,
			timestamp: timestamp,
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		},
		key:   key,
		value: value,
	}
}

func (db *LograDB) writeKeyPair(key string, val string) error {

	writer := bufio.NewWriter(db.activeFile)

	offset, err := db.activeFile.Seek(0, io.SeekEnd)

	if err != nil {
		fmt.Println("Error seeking to end of file:", err)
		return err
	}

	_, err = writer.WriteString(fmt.Sprintf("%s:%d:%s\n", key, offset, val))
	if err != nil {
		fmt.Println("Error writing key-value pair:", err)
		return err
	}

	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing writer:", err)
		return err
	}
	db.keyDict[key] = offset
	return nil

}

func NewLograDB(db_file_path string, version string) (*LograDB, error) {
	var logra_db *LograDB

	dbFile, err := os.OpenFile(db_file_path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error creating/opening database file:", err)
		return nil, err
	}
	logra_db = &LograDB{
		keyDict:    make(map[string]ObjLayout),
		dbFilePath: db_file_path,
		version:    version,
		activeFile: dbFile,
	}
	populated, err := logra_db.populateAllKeys()
	if populated {
		fmt.Println("All keys populated successfully.")
	} else {
		fmt.Println("Failed to populate keys:", err)
		return nil, err
	}

	return logra_db, nil

}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <db_file_path> command [args...]")
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
			value, err := lograDB.GetValue(key)
			if err != nil {
				fmt.Println("Error retrieving value:", err)
				return
			}
			fmt.Printf("Key '%s' exists in the database with value '%s'.\n", key, value)
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
