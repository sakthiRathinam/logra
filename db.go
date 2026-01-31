package logra

import (
	"fmt"
	"io"
	"sync"

	"sakthirathinam/logra/internal/index"
	"sakthirathinam/logra/internal/storage"

	"github.com/gofrs/flock"
)

const lograLockFile = "logra.lock"

type LograDB struct {
	Index   *index.Index
	Storage *storage.Storage
	version string
	Mutex   sync.RWMutex
	Flock   *flock.Flock
}

type Record struct {
	Key       string
	Value     string
	Timestamp int64
}

func Open(path string, version string) (*LograDB, error) {
	// flock := flock.New(filepath.Join(path, lograLockFile))

	// locked, err := flock.TryLock()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to acquire lock: %w", err)
	// }
	// if !locked {
	// 	return nil, fmt.Errorf("failed to acquire lock")
	// }

	store, err := storage.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	idx := index.New()

	db := &LograDB{
		Index:   idx,
		Storage: store,
		version: version,
		Flock:   nil,
	}

	if err := db.loadIndex(); err != nil {
		store.Close()
		db.Flock.Unlock()
		db.Flock.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return db, nil
}

func (db *LograDB) Close() error {
	err := db.Storage.Close()
	// if err != nil {
	// 	return fmt.Errorf("failed to close storage: %w", err)
	// // }
	// err = db.Flock.Unlock()
	// if err != nil {
	// 	return fmt.Errorf("failed to release lock: %w", err)
	// }
	// db.Flock.Close()
	return err
}

func (db *LograDB) loadIndex() error {
	onAppend := func(offset int64, key []byte, header storage.Header, fileID int, reader io.Reader) error {
		db.Index.Add(string(key), index.Entry{
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
		db.Index.Remove(string(key))
	}

	return db.Storage.Scan(onAppend, onDelete)

}
func (db *LograDB) SwapIndex(newIndex *index.Index) {
	db.Index = newIndex
}

func (db *LograDB) Version() string {
	return db.version
}

func (db *LograDB) Has(key string) bool {
	return db.Index.Has(key)
}

func (db *LograDB) Delete(key string) error {
	if !db.Index.Has(key) {
		return fmt.Errorf("key not found")
	}
	deleted := db.Index.Remove(key)
	if !deleted {
		return fmt.Errorf("key not found")
	}

	db.Storage.MarkDeleted([]byte(key))
	return nil
}
func (db *LograDB) Get(key string) (Record, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()
	entry, exists := db.Index.Lookup(key)
	if !exists {
		return Record{}, fmt.Errorf("key not found")
	}

	header := storage.Header{
		CRC:       entry.CRC,
		Timestamp: entry.Timestamp,
		KeySize:   entry.KeySize,
		ValueSize: entry.ValueSize,
	}

	rec, err := db.Storage.ReadAtFile(entry.Offset, header, entry.FileID)
	if err != nil {
		return Record{}, err
	}

	return Record{
		Key:       string(rec.Key),
		Value:     string(rec.Value),
		Timestamp: rec.Header.Timestamp,
	}, nil
}

func (db *LograDB) Set(key, value string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	fileID := db.Storage.ActiveFileID()
	offset, header, err := db.Storage.Append([]byte(key), []byte(value))
	if err != nil {
		return err
	}

	db.Index.Add(key, index.Entry{
		Offset:    offset,
		CRC:       header.CRC,
		Timestamp: header.Timestamp,
		KeySize:   header.KeySize,
		ValueSize: header.ValueSize,
		FileID:    fileID,
	})

	return nil
}
