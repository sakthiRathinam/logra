package compact

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"sakthirathinam/logra"
	"sakthirathinam/logra/internal/index"
	"sakthirathinam/logra/internal/storage"
)

type Compact struct {
	dbObj          *logra.LograDB
	maxFileId      int
	sortedFileObjs []*os.File
	compactStatus  CompactStatus
	compactIndex   *index.Index
	mergeFile      *os.File
	mergeFileId    int
}

type CompactStatus string

const (
	CompactInitialized CompactStatus = "initialized"
	CompactInProgress  CompactStatus = "in_progress"
	CompactCompleted   CompactStatus = "completed"
)

type mergeState struct {
	Status         string `json:"status"`
	MaxFileId      int    `json:"maxFileId"`
	MergeFileCount int    `json:"mergeFileCount"`
}

func NewCompact(lograDb *logra.LograDB) *Compact {
	return &Compact{
		dbObj:         lograDb,
		compactStatus: CompactInitialized,
		compactIndex:  index.New(),
	}
}

func (m *Compact) Execute() error {
	if err := m.Prepare(); err != nil {
		return err
	}

	for _, fileObj := range m.sortedFileObjs {
		if err := m.processFile(fileObj); err != nil {
			return err
		}
	}

	if m.mergeFile != nil {
		m.mergeFile.Close()
		m.mergeFile = nil
	}

	// Delete old .dat files (0 through maxFileId)
	if err := m.deleteOldFiles(); err != nil {
		return err
	}

	// Rename merge files to regular .dat files
	if err := m.renameMergeFiles(); err != nil {
		return err
	}

	// Build final index: start with compactIndex, then scan files after maxFileId
	if err := m.scanNewFiles(); err != nil {
		return err
	}

	// Swap the index
	m.dbObj.SwapIndex(m.compactIndex)

	m.compactStatus = CompactCompleted

	// Clean up state file
	os.Remove(filepath.Join(m.dbObj.Storage.Dir, "merge.json"))

	return nil
}

func (m *Compact) Prepare() error {
	datFiles, err := m.dbObj.Storage.GetAllDatFiles()
	if err != nil {
		return err
	}
	if len(datFiles) == 0 {
		return nil
	}
	lastFileId, err := storage.ParseFileIDFromName(filepath.Base(datFiles[len(datFiles)-1].Name()))
	if err != nil {
		return err
	}
	m.maxFileId = lastFileId
	m.sortedFileObjs = datFiles
	m.compactStatus = CompactInProgress

	// Write state before starting
	if err := m.writeState(CompactInProgress); err != nil {
		return err
	}

	// Switch active file so new writes go to maxFileId+1
	if err := changeActiveFile(m.dbObj, m.maxFileId+1); err != nil {
		return err
	}

	// Create the first merge file
	return m.createMergeFile(0)
}

func (m *Compact) createMergeFile(id int) error {
	path := filepath.Join(m.dbObj.Storage.Dir, fmt.Sprintf("merge_%d.dat", id))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if m.mergeFile != nil {
		m.mergeFile.Close()
	}
	m.mergeFile = f
	m.mergeFileId = id
	return nil
}

func (m *Compact) rotateMergeFileIfNeeded() error {
	info, err := m.mergeFile.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= storage.MaxDataFileSize*4 {
		return m.createMergeFile(m.mergeFileId + 1)
	}
	return nil
}

func (m *Compact) appendToMergeFile(key, value []byte) (int64, storage.Header, error) {
	offset, err := m.mergeFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, storage.Header{}, err
	}

	data := storage.EncodeRecord(key, value)
	writer := bufio.NewWriter(m.mergeFile)
	if _, err := writer.Write(data); err != nil {
		return 0, storage.Header{}, err
	}
	if err := writer.Flush(); err != nil {
		return 0, storage.Header{}, err
	}

	header, err := storage.DecodeHeader(data[:storage.HeaderSize])
	if err != nil {
		return 0, storage.Header{}, err
	}

	if err := m.rotateMergeFileIfNeeded(); err != nil {
		return 0, storage.Header{}, err
	}

	return offset, header, nil
}

func (m *Compact) processFile(fileObj *os.File) error {
	onAppend := func(offset int64, key []byte, header storage.Header, fileID int, reader io.Reader) error {
		existingEntry, exists := m.dbObj.Index.Lookup(string(key))

		if exists && existingEntry.FileID == fileID && existingEntry.Offset == offset {
			// This is the live record — read value from reader and write to merge file
			value := make([]byte, header.ValueSize)
			if _, err := io.ReadFull(reader, value); err != nil {
				return err
			}

			newOffset, newHeader, err := m.appendToMergeFile(key, value)
			if err != nil {
				return err
			}

			m.compactIndex.Add(string(key), index.Entry{
				Offset:    newOffset,
				CRC:       newHeader.CRC,
				Timestamp: newHeader.Timestamp,
				KeySize:   newHeader.KeySize,
				ValueSize: newHeader.ValueSize,
				FileID:    m.mergeFileId,
			})
			return nil
		}

		// Stale record — skip value bytes
		if _, err := io.CopyN(io.Discard, reader, int64(header.ValueSize)); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		return nil
	}

	onDelete := func(key []byte, header storage.Header) {
		// Tombstones are dropped — not written to merge files
	}

	return m.dbObj.Storage.ScanFile(fileObj, false, onAppend, onDelete)
}

func (m *Compact) deleteOldFiles() error {
	for i := 0; i <= m.maxFileId; i++ {
		path := filepath.Join(m.dbObj.Storage.Dir, strconv.Itoa(i)+".dat")
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (m *Compact) renameMergeFiles() error {
	for i := 0; i <= m.mergeFileId; i++ {
		src := filepath.Join(m.dbObj.Storage.Dir, fmt.Sprintf("merge_%d.dat", i))
		dst := filepath.Join(m.dbObj.Storage.Dir, fmt.Sprintf("%d.dat", i))
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}

	// Update compactIndex file IDs — they currently reference mergeFileId,
	// but after rename the file IDs are the same numbers, so no change needed.
	return nil
}

func (m *Compact) scanNewFiles() error {
	onAppend := func(offset int64, key []byte, header storage.Header, fileID int, reader io.Reader) error {
		m.compactIndex.Add(string(key), index.Entry{
			Offset:    offset,
			CRC:       header.CRC,
			Timestamp: header.Timestamp,
			KeySize:   header.KeySize,
			ValueSize: header.ValueSize,
			FileID:    fileID,
		})
		return nil
	}

	onDelete := func(key []byte, header storage.Header) {
		m.compactIndex.Remove(string(key))
	}

	return m.dbObj.Storage.ScanFilesAfter(m.maxFileId, onAppend, onDelete)
}

func (m *Compact) writeState(status CompactStatus) error {
	state := mergeState{
		Status:    string(status),
		MaxFileId: m.maxFileId,
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.dbObj.Storage.Dir, "merge.json"), data, 0644)
}

func changeActiveFile(lograDb *logra.LograDB, newFileId int) error {
	newDataFilePath := filepath.Join(lograDb.Storage.Dir, strconv.Itoa(newFileId)+".dat")
	datFile, err := os.OpenFile(newDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	lograDb.Storage.ActiveFile = datFile
	return nil
}

// RecoverIfNeeded checks for a half-baked merge and cleans up merge files.
func RecoverIfNeeded(dir string) error {
	stateFile := filepath.Join(dir, "merge.json")
	data, err := os.ReadFile(stateFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var state mergeState
	if err := json.Unmarshal(data, &state); err != nil {
		// Corrupted state file, clean up
		return cleanupMergeFiles(dir, stateFile)
	}

	if state.Status != string(CompactCompleted) {
		return cleanupMergeFiles(dir, stateFile)
	}

	// Completed state file left behind, just remove it
	return os.Remove(stateFile)
}

func cleanupMergeFiles(dir, stateFile string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) > 6 && name[:6] == "merge_" && filepath.Ext(name) == ".dat" {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return os.Remove(stateFile)
}
