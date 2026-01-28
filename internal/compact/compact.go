package compact

import (
	"os"
	"path/filepath"
	"sakthirathinam/logra"
	"sakthirathinam/logra/internal/index"
	"sakthirathinam/logra/internal/storage"
	"strconv"
)

type Compact struct {
	dbObj          *logra.LograDB
	maxFileId      int
	currentIndex   *index.Index
	sortedFileObjs []*os.File
	compactStatus  CompactStatus
	compactIndex   *index.Index
}

type CompactStatus string

const (
	CompactInitialized CompactStatus = "initialized"
	CompactInProgress  CompactStatus = "in_progress"
	CompactCompleted   CompactStatus = "completed"
)

func NewCompact(lograDb *logra.LograDB, currentIndex *index.Index) *Compact {
	return &Compact{
		dbObj:         lograDb,
		currentIndex:  currentIndex,
		compactStatus: CompactInitialized,
		compactIndex:  index.New(),
	}
}

func (m *Compact) Prepare() error {
	// Implementation for preparing the merge process
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

	err = changeActiveFile(m.dbObj, m.maxFileId+1)

	return err
}

func (m *Compact) Execute() error {

	err := m.Prepare()
	if err != nil {
		return err
	}
	for _, fileObj := range m.sortedFileObjs {
		err := m.processFile(fileObj)
		if err != nil {
			return err
		}
	}
	m.compactStatus = CompactCompleted
	return nil
}

func (m *Compact) processFile(fileObj *os.File) error {

	return nil

}
func changeActiveFile(lograDb *logra.LograDB, newFileId int) error {
	newDataFilePath := lograDb.Storage.Dir + "/" + strconv.Itoa(newFileId) + ".dat"
	datFile, err := os.OpenFile(newDataFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	lograDb.Storage.ActiveFile = datFile
	return nil
}
