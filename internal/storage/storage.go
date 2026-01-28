package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

/*
**
Data Storage Layer
key value pairs will be stored in multiple files if they exceed a certain size limit (e.g., 250mb per file).
Data file format - logra-<file_number>.dat
Hint file format - logra-<file_number>.hint
**
*/
const MaxDataFileSize = 1 * 1024 * 1024 // 250 MB

type Storage struct {
	ActiveFile *os.File
	Dir        string
}

func Open(dirPath string) (*Storage, error) {
	var activeFile *os.File
	var err error

	activeFile, err = getActiveFile(dirPath)
	if err != nil {
		return nil, err
	}

	return &Storage{
		ActiveFile: activeFile,
		Dir:        dirPath,
	}, nil
}

func getActiveFile(path string) (*os.File, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return nil, err
		}
		activeFile, err := os.OpenFile(path+"/0.dat", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		return activeFile, nil
	}
	return findActiveFileInDir(path)
}

func (s *Storage) Close() error {
	return s.ActiveFile.Close()
}

func findActiveFileInDir(path string) (*os.File, error) {
	files, err := os.ReadDir(path)

	if len(files) == 0 {
		activeFile, err := os.OpenFile(path+"/0.dat", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		return activeFile, nil
	}

	if err != nil {
		return nil, err
	}

	datExtFiles := []os.DirEntry{}
	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if !file.IsDir() && len(baseName) > 4 && baseName[len(baseName)-4:] == ".dat" {
			datExtFiles = append(datExtFiles, file)
		}
	}
	fmt.Println("Data files found:", len(datExtFiles))
	if len(datExtFiles) == 0 {
		fmt.Println("No .dat files found, creating new data file.")
		activeFile, err := os.OpenFile(path+"/0.dat", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		return activeFile, nil
	}

	currentMaxFileEntry := struct {
		file       os.DirEntry
		segmentNum int
	}{segmentNum: -1}

	for _, file := range datExtFiles {
		baseName := filepath.Base(file.Name())
		fmt.Println("Found data file:", baseName)
		fmt.Println("Parsing segment number from file name:", baseName)
		segmentSplit := strings.Split(baseName, ".")
		if len(segmentSplit) != 2 {
			continue
		}
		segmentNum, err := strconv.Atoi(segmentSplit[0])
		if err != nil {
			continue
		}

		if segmentNum > currentMaxFileEntry.segmentNum {
			currentMaxFileEntry.file = file
			currentMaxFileEntry.segmentNum = segmentNum
		}
	}

	if currentMaxFileEntry.segmentNum == -1 {
		return nil, fmt.Errorf("no valid data files found")
	}
	return os.OpenFile(path+"/"+filepath.Base(currentMaxFileEntry.file.Name()), os.O_RDWR|os.O_APPEND, 0666)
}

func (s *Storage) SwitchNewDatFile() error {
	newSegmentNum := 0
	currentFileName := filepath.Base(s.ActiveFile.Name())
	segmentSplit := strings.Split(currentFileName, ".")
	if len(segmentSplit) == 2 {
		currentSegmentNum, err := strconv.Atoi(segmentSplit[0])
		if err != nil {
			return err
		}
		newSegmentNum = currentSegmentNum + 1
	}

	createDatFile, err := os.OpenFile(s.Dir+"/"+strconv.Itoa(newSegmentNum)+".dat", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	s.ActiveFile.Close()
	s.ActiveFile = createDatFile
	return nil
}

func (s *Storage) MarkDeleted(key []byte) error {
	_, _, err := s.Append(key, []byte{})
	return err
}

func (s *Storage) Append(key, value []byte) (int64, Header, error) {
	offset, err := s.ActiveFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, Header{}, err
	}

	data := EncodeRecord(key, value)
	writer := bufio.NewWriter(s.ActiveFile)

	if _, err := writer.Write(data); err != nil {
		return 0, Header{}, err
	}
	if err := writer.Flush(); err != nil {
		return 0, Header{}, err
	}

	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return 0, Header{}, err
	}

	activeFileInfo, err := s.ActiveFile.Stat()
	if err != nil {
		return 0, Header{}, err
	}

	if activeFileInfo.Size() >= MaxDataFileSize {
		if err := s.SwitchNewDatFile(); err != nil {
			return 0, Header{}, err
		}
	}
	return offset, header, nil
}

func (s *Storage) ReadAt(offset int64, header Header) (Record, error) {
	if _, err := s.ActiveFile.Seek(offset, io.SeekStart); err != nil {
		return Record{}, err
	}

	reader := bufio.NewReader(s.ActiveFile)
	recordSize := HeaderSize + header.KeySize + header.ValueSize
	data := make([]byte, recordSize)

	if _, err := io.ReadFull(reader, data); err != nil {
		return Record{}, err
	}

	return DecodeRecord(data)
}

func (s *Storage) GetAllDatFiles() ([]*os.File, error) {
	files, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, err
	}

	datFiles := []*os.File{}
	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if !file.IsDir() && len(baseName) > 4 && baseName[len(baseName)-4:] == ".dat" {
			f, err := os.OpenFile(s.Dir+"/"+baseName, os.O_RDWR, 0666)
			if err != nil {
				return nil, err
			}
			datFiles = append(datFiles, f)
		}
	}
	sort.Slice(datFiles, func(i, j int) bool {
		id1, err1 := ParseFileIDFromName(filepath.Base(datFiles[i].Name()))
		id2, err2 := ParseFileIDFromName(filepath.Base(datFiles[j].Name()))
		if err1 != nil || err2 != nil {
			return false
		}
		return id1 < id2
	})
	return datFiles, nil
}

func ParseFileIDFromName(fileName string) (int, error) {
	baseName := filepath.Base(fileName)
	segmentSplit := strings.Split(baseName, ".")
	if len(segmentSplit) != 2 {
		return -1, fmt.Errorf("invalid file name format")
	}
	segmentNum, err := strconv.Atoi(segmentSplit[0])
	if err != nil {
		return -1, err
	}
	return segmentNum, nil
}

func (s *Storage) scanFile(file *os.File, onAppend func(offset int64, key []byte, header Header, fileID int) error, onDelete func(key []byte, header Header)) error {
	reader := bufio.NewReader(file)
	offset := int64(0)
	fileID, err := ParseFileIDFromName(filepath.Base(file.Name()))
	if err != nil {
		return err
	}
	for {
		headerBytes := make([]byte, HeaderSize)
		if _, err := io.ReadFull(reader, headerBytes); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		keySize := binary.LittleEndian.Uint32(headerBytes[4:8])
		valueSize := binary.LittleEndian.Uint32(headerBytes[8:12])

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		header, err := DecodeHeader(headerBytes)
		if err != nil {
			return err
		}
		if valueSize == 0 {
			onDelete(key, header)
		} else if err := onAppend(offset, key, header, fileID); err != nil {
			fmt.Printf("Error in scan function%s: for this key %s\n", err, string(key))
		}

		if _, err := io.CopyN(io.Discard, reader, int64(valueSize)); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		offset += int64(HeaderSize + keySize + valueSize)
	}

}

func (s *Storage) Scan(onAppend func(offset int64, key []byte, header Header, fileID int) error, onDelete func(key []byte, header Header)) error {
	files, err := s.GetAllDatFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		if err := s.scanFile(f, onAppend, onDelete); err != nil {
			return err
		}
	}

	return nil
}
