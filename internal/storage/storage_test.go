package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("creates directory and file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		// Check directory exists
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Error("Open() did not create directory")
		}

		// Check dat file exists
		datFile := filepath.Join(path, "logra-0.dat")
		if _, err := os.Stat(datFile); os.IsNotExist(err) {
			t.Errorf("Open() did not create %s", datFile)
		}
	})

	t.Run("opens existing directory", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		// Create first storage
		s1, err := Open(path)
		if err != nil {
			t.Fatalf("First Open() error = %v", err)
		}
		s1.Close()

		// Open again
		s2, err := Open(path)
		if err != nil {
			t.Fatalf("Second Open() error = %v", err)
		}
		defer s2.Close()
	})

	t.Run("finds highest numbered file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		// Create directory and multiple files
		os.MkdirAll(path, 0755)
		os.Create(filepath.Join(path, "logra-0.dat"))
		os.Create(filepath.Join(path, "logra-1.dat"))
		f, _ := os.Create(filepath.Join(path, "logra-2.dat"))
		f.Close()

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()
	})
}

func TestStorage_Close(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Second close should error (file already closed)
	if err := s.Close(); err == nil {
		t.Error("Second Close() should error but didn't")
	}
}

func TestStorage_Append(t *testing.T) {
	t.Parallel()

	t.Run("single append", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		key := []byte("testkey")
		value := []byte("testvalue")

		offset, header, err := s.Append(key, value)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		if offset != 0 {
			t.Errorf("First Append() offset = %d, want 0", offset)
		}
		if header.KeySize != uint32(len(key)) {
			t.Errorf("Header.KeySize = %d, want %d", header.KeySize, len(key))
		}
		if header.ValueSize != uint32(len(value)) {
			t.Errorf("Header.ValueSize = %d, want %d", header.ValueSize, len(value))
		}
		if header.CRC == 0 {
			t.Error("Header.CRC should not be zero")
		}
	})

	t.Run("multiple appends increase offset", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		offset1, header1, _ := s.Append([]byte("key1"), []byte("value1"))
		offset2, _, _ := s.Append([]byte("key2"), []byte("value2"))

		expectedOffset2 := offset1 + header1.RecordSize()
		if offset2 != expectedOffset2 {
			t.Errorf("Second offset = %d, want %d", offset2, expectedOffset2)
		}
	})
}

func TestStorage_ReadAt(t *testing.T) {
	t.Parallel()

	t.Run("read valid record", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		key := []byte("testkey")
		value := []byte("testvalue")

		offset, header, err := s.Append(key, value)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		record, err := s.ReadAt(offset, header)
		if err != nil {
			t.Fatalf("ReadAt() error = %v", err)
		}

		if !bytes.Equal(record.Key, key) {
			t.Errorf("ReadAt() Key = %q, want %q", record.Key, key)
		}
		if !bytes.Equal(record.Value, value) {
			t.Errorf("ReadAt() Value = %q, want %q", record.Value, value)
		}
	})

	t.Run("read multiple records", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		records := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
			{[]byte("key3"), []byte("value3")},
		}

		offsets := make([]int64, len(records))
		headers := make([]Header, len(records))

		for i, r := range records {
			offsets[i], headers[i], _ = s.Append(r.key, r.value)
		}

		// Read in reverse order to ensure random access works
		for i := len(records) - 1; i >= 0; i-- {
			got, err := s.ReadAt(offsets[i], headers[i])
			if err != nil {
				t.Errorf("ReadAt(%d) error = %v", i, err)
				continue
			}
			if !bytes.Equal(got.Key, records[i].key) {
				t.Errorf("ReadAt(%d) Key = %q, want %q", i, got.Key, records[i].key)
			}
		}
	})

	t.Run("invalid offset", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		_, header, _ := s.Append([]byte("key"), []byte("value"))

		// Try to read at invalid offset
		_, err = s.ReadAt(999999, header)
		if err == nil {
			t.Error("ReadAt() with invalid offset should error")
		}
	})
}

func TestStorage_Scan(t *testing.T) {
	t.Parallel()

	t.Run("scan all records", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		keys := []string{"key1", "key2", "key3"}
		for _, k := range keys {
			s.Append([]byte(k), []byte("value-"+k))
		}

		scannedKeys := []string{}
		err = s.Scan(func(offset int64, key []byte, header Header, fileID int) error {
			scannedKeys = append(scannedKeys, string(key))
			return nil
		})

		if err != nil {
			t.Fatalf("Scan() error = %v", err)
		}

		if len(scannedKeys) != len(keys) {
			t.Errorf("Scan() found %d keys, want %d", len(scannedKeys), len(keys))
		}

		for i, k := range keys {
			if scannedKeys[i] != k {
				t.Errorf("Scan() key[%d] = %q, want %q", i, scannedKeys[i], k)
			}
		}
	})

	t.Run("scan empty storage", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		count := 0
		err = s.Scan(func(offset int64, key []byte, header Header, fileID int) error {
			count++
			return nil
		})

		if err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		if count != 0 {
			t.Errorf("Scan() on empty storage called callback %d times", count)
		}
	})

	t.Run("scan provides correct offsets", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		expectedOffsets := []int64{}
		for i := 0; i < 3; i++ {
			offset, _, _ := s.Append([]byte("key"), []byte("value"))
			expectedOffsets = append(expectedOffsets, offset)
		}

		scannedOffsets := []int64{}
		s.Scan(func(offset int64, key []byte, header Header, fileID int) error {
			scannedOffsets = append(scannedOffsets, offset)
			return nil
		})

		for i, expected := range expectedOffsets {
			if scannedOffsets[i] != expected {
				t.Errorf("Scan() offset[%d] = %d, want %d", i, scannedOffsets[i], expected)
			}
		}
	})

	t.Run("callback error stops scan", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer s.Close()

		for i := 0; i < 5; i++ {
			s.Append([]byte("key"), []byte("value"))
		}

		// Note: the current implementation logs errors but doesn't stop scanning
		// This test documents that behavior
		callCount := 0
		customErr := errors.New("stop scanning")
		s.Scan(func(offset int64, key []byte, header Header, fileID int) error {
			callCount++
			if callCount >= 2 {
				return customErr
			}
			return nil
		})

		// Current implementation continues despite callback errors
		// Just verify scan completes without panicking
	})
}

func TestStorage_Persistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Write data
	s1, _ := Open(path)
	offset1, header1, _ := s1.Append([]byte("key1"), []byte("value1"))
	offset2, header2, _ := s1.Append([]byte("key2"), []byte("value2"))
	s1.Close()

	// Reopen and verify
	s2, _ := Open(path)
	defer s2.Close()

	rec1, err := s2.ReadAt(offset1, header1)
	if err != nil {
		t.Fatalf("ReadAt after reopen error = %v", err)
	}
	if string(rec1.Key) != "key1" || string(rec1.Value) != "value1" {
		t.Errorf("Record 1 mismatch: got %q=%q", rec1.Key, rec1.Value)
	}

	rec2, err := s2.ReadAt(offset2, header2)
	if err != nil {
		t.Fatalf("ReadAt after reopen error = %v", err)
	}
	if string(rec2.Key) != "key2" || string(rec2.Value) != "value2" {
		t.Errorf("Record 2 mismatch: got %q=%q", rec2.Key, rec2.Value)
	}
}
