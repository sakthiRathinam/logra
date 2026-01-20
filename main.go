package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"time"
)

type LograDB struct {
	keyDict    map[string]InMemoryObj
	dbFilePath string
	version    string
	activeFile *os.File
}

type InMemoryObj struct {
	offset       int64
	recordHeader RecordHeader
}
type RecordHeader struct {
	crc       uint32
	timestamp int64
	keySize   uint32
	valueSize uint32
}
type DBRow struct {
	header RecordHeader
	key    string
	value  string
}

func (db *LograDB) GetVersion() string {
	return db.version
}

func tranformBytesToInMemoryVal(offset int64, data []byte) (InMemoryObj, error) {
	var imv InMemoryObj
	buffer := bytes.NewReader(data)

	err := binary.Read(buffer, binary.LittleEndian, &imv.recordHeader.crc)
	if err != nil {
		return imv, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &imv.recordHeader.keySize)
	if err != nil {
		return imv, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &imv.recordHeader.valueSize)
	if err != nil {
		return imv, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &imv.recordHeader.timestamp)
	if err != nil {
		return imv, err
	}
	imv.offset = offset
	return imv, nil
}
func transformBytesToDBRow(data []byte) (DBRow, error) {
	var row DBRow
	buffer := bytes.NewReader(data)

	err := binary.Read(buffer, binary.LittleEndian, &row.header.crc)
	if err != nil {
		return row, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &row.header.keySize)
	if err != nil {
		return row, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &row.header.valueSize)
	if err != nil {
		return row, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &row.header.timestamp)
	if err != nil {
		return row, err
	}

	keyBytes := make([]byte, row.header.keySize)
	_, err = buffer.Read(keyBytes)
	if err != nil {
		return row, err
	}
	row.key = string(keyBytes)

	valueBytes := make([]byte, row.header.valueSize)
	_, err = buffer.Read(valueBytes)
	if err != nil {
		return row, err
	}
	row.value = string(valueBytes)

	return row, nil
}
func (db *LograDB) populateAllKeys() (bool, error) {
	bufferedReader := bufio.NewReader(db.activeFile)
	offset := int64(0)
	for {
		headerBytes := make([]byte, 16)
		_, err := io.ReadFull(bufferedReader, headerBytes)
		if err == io.EOF {
			break
		}
		keySize := binary.LittleEndian.Uint32(headerBytes[4:8])
		valueSize := binary.LittleEndian.Uint32(headerBytes[8:12])

		totalRecordSize := int64(16 + keySize + valueSize)
		key := make([]byte, keySize)
		_, err = io.ReadFull(bufferedReader, key)
		if err != nil {
			return false, err
		}
		// Skip the value bytes

		_, err = bufferedReader.Discard(int(valueSize))
		if err != nil {
			return false, err
		}
		inMemObj, err := tranformBytesToInMemoryVal(offset, headerBytes)
		if err != nil {
			return false, err
		}
		db.keyDict[string(key)] = inMemObj
		offset += totalRecordSize
	}
	return true, nil
}

func (db *LograDB) HasKey(key string) bool {
	_, exists := db.keyDict[key]
	return exists
}

func (db *LograDB) GetValue(key string) (string, error) {
	memVal, exists := db.keyDict[key]
	if !exists {
		return "", fmt.Errorf("key not found")
	}

	_, err := db.activeFile.Seek(memVal.offset, io.SeekStart)
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

func (db *LograDB) writeKeyPair(key string, val string) error {
	writer := bufio.NewWriter(db.activeFile)
	offset, err := db.activeFile.Seek(0, io.SeekEnd)

	buffer := bytes.Buffer{}
	timestamp := time.Now().Unix()
	binary.Write(&buffer, binary.LittleEndian, uint32(len(key)))
	binary.Write(&buffer, binary.LittleEndian, uint32(len(val)))
	binary.Write(&buffer, binary.LittleEndian, int64(timestamp))
	buffer.Write([]byte(key))
	buffer.Write([]byte(val))

	payload := buffer.Bytes()

	crc := crc32.ChecksumIEEE(payload)

	finalBuffer := bytes.Buffer{}
	binary.Write(&finalBuffer, binary.LittleEndian, crc)
	finalBuffer.Write(payload)

	_, err = writer.Write(finalBuffer.Bytes())
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	db.keyDict[key] = InMemoryObj{
		offset: offset,
		recordHeader: RecordHeader{
			crc:       crc,
			timestamp: timestamp,
			keySize:   uint32(len(key)),
			valueSize: uint32(len(val)),
		},
	}
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
		keyDict:    make(map[string]InMemoryObj),
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
