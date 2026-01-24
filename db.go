package logra

import (
	"fmt"

	"sakthirathinam/logra/internal/index"
	"sakthirathinam/logra/internal/storage"
)

type LograDB struct {
	index   *index.Index
	storage *storage.Storage
	version string
}

type Record struct {
	Key       string
	Value     string
	Timestamp int64
}

func Open(path string, version string) (*LograDB, error) {
	store, err := storage.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	idx := index.New()

	db := &LograDB{
		index:   idx,
		storage: store,
		version: version,
	}

	if err := db.loadIndex(); err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return db, nil
}

func (db *LograDB) loadIndex() error {
	return db.storage.Scan(func(offset int64, key []byte, header storage.Header, fileID int) error {
		db.index.Add(string(key), index.Entry{
			Offset:    offset,
			CRC:       header.CRC,
			Timestamp: header.Timestamp,
			KeySize:   header.KeySize,
			ValueSize: header.ValueSize,
			FileID:    fileID,
		})
		return nil
	})
}

func (db *LograDB) Close() error {
	return db.storage.Close()
}

func (db *LograDB) Version() string {
	return db.version
}

func (db *LograDB) Has(key string) bool {
	return db.index.Has(key)
}

func (db *LograDB) Get(key string) (Record, error) {
	entry, exists := db.index.Lookup(key)
	if !exists {
		return Record{}, fmt.Errorf("key not found")
	}

	header := storage.Header{
		CRC:       entry.CRC,
		Timestamp: entry.Timestamp,
		KeySize:   entry.KeySize,
		ValueSize: entry.ValueSize,
	}

	rec, err := db.storage.ReadAt(entry.Offset, header)
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
	offset, header, err := db.storage.Append([]byte(key), []byte(value))
	if err != nil {
		return err
	}

	db.index.Add(key, index.Entry{
		Offset:    offset,
		CRC:       header.CRC,
		Timestamp: header.Timestamp,
		KeySize:   header.KeySize,
		ValueSize: header.ValueSize,
	})

	return nil
}
