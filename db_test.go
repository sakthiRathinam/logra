package logra

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("creates new database", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, err := Open(path, "1.0.0")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if db == nil {
			t.Fatal("Open() returned nil database")
		}
	})

	t.Run("loads existing data", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		// Create and populate database
		db1, err := Open(path, "1.0.0")
		if err != nil {
			t.Fatalf("First Open() error = %v", err)
		}
		db1.Set("key1", "value1")
		db1.Set("key2", "value2")
		db1.Close()

		// Reopen and verify
		db2, err := Open(path, "1.0.0")
		if err != nil {
			t.Fatalf("Second Open() error = %v", err)
		}
		defer db2.Close()

		if !db2.Has("key1") {
			t.Error("key1 not found after reopen")
		}
		if !db2.Has("key2") {
			t.Error("key2 not found after reopen")
		}
	})

	t.Run("sets version", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, err := Open(path, "2.0.0")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if db.Version() != "2.0.0" {
			t.Errorf("Version() = %q, want %q", db.Version(), "2.0.0")
		}
	})
}

func TestLograDB_Close(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestLograDB_Version(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{"semantic version", "1.0.0"},
		{"simple version", "v1"},
		{"empty version", ""},
		{"custom version", "alpha-123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "testdb")

			db, err := Open(path, tt.version)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			if got := db.Version(); got != tt.version {
				t.Errorf("Version() = %q, want %q", got, tt.version)
			}
		})
	}
}

func TestLograDB_Has(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Before Set
	if db.Has("nonexistent") {
		t.Error("Has() should return false for non-existent key")
	}

	// After Set
	db.Set("mykey", "myvalue")
	if !db.Has("mykey") {
		t.Error("Has() should return true after Set")
	}

	// Empty key
	if db.Has("") {
		t.Error("Has() should return false for empty key")
	}
}

func TestLograDB_Get(t *testing.T) {
	t.Parallel()

	t.Run("missing key returns error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		_, err := db.Get("nonexistent")
		if err == nil {
			t.Error("Get() should return error for missing key")
		}
	})

	t.Run("returns correct record after Set", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		db.Set("mykey", "myvalue")

		rec, err := db.Get("mykey")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if rec.Key != "mykey" {
			t.Errorf("Record.Key = %q, want %q", rec.Key, "mykey")
		}
		if rec.Value != "myvalue" {
			t.Errorf("Record.Value = %q, want %q", rec.Value, "myvalue")
		}
		if rec.Timestamp <= 0 {
			t.Error("Record.Timestamp should be positive")
		}
	})

	t.Run("get multiple keys", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for k, v := range testData {
			db.Set(k, v)
		}

		for k, expectedV := range testData {
			rec, err := db.Get(k)
			if err != nil {
				t.Errorf("Get(%q) error = %v", k, err)
				continue
			}
			if rec.Value != expectedV {
				t.Errorf("Get(%q).Value = %q, want %q", k, rec.Value, expectedV)
			}
		}
	})
}

func TestLograDB_Set(t *testing.T) {
	t.Parallel()

	t.Run("new key succeeds", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		err := db.Set("newkey", "newvalue")
		if err != nil {
			t.Errorf("Set() error = %v", err)
		}

		if !db.Has("newkey") {
			t.Error("Key should exist after Set")
		}
	})

	t.Run("update key succeeds", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		db.Set("key", "value1")
		err := db.Set("key", "value2")
		if err != nil {
			t.Errorf("Set() update error = %v", err)
		}

		rec, _ := db.Get("key")
		if rec.Value != "value2" {
			t.Errorf("Updated value = %q, want %q", rec.Value, "value2")
		}
	})

	t.Run("single character value", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		// Single character value should work
		err := db.Set("key", "v")
		if err != nil {
			t.Errorf("Set() with single char value error = %v", err)
		}

		rec, _ := db.Get("key")
		if rec.Value != "v" {
			t.Errorf("Single char value not stored correctly: got %q", rec.Value)
		}
	})

	t.Run("unicode key and value", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "testdb")

		db, _ := Open(path, "1.0.0")
		defer db.Close()

		err := db.Set("日本語キー", "中文值")
		if err != nil {
			t.Errorf("Set() with unicode error = %v", err)
		}

		rec, _ := db.Get("日本語キー")
		if rec.Value != "中文值" {
			t.Errorf("Unicode value = %q, want %q", rec.Value, "中文值")
		}
	})
}

func TestLograDB_Persistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Write data
	db1, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("First Open() error = %v", err)
	}

	testData := map[string]string{
		"key1":   "value1",
		"key2":   "value2",
		"key3":   "value3",
		"日本語":    "japanese",
		"emoji✨": "sparkle",
	}

	for k, v := range testData {
		if err := db1.Set(k, v); err != nil {
			t.Fatalf("Set(%q) error = %v", k, err)
		}
	}
	db1.Close()

	// Reopen and verify
	db2, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Second Open() error = %v", err)
	}
	defer db2.Close()

	for k, expectedV := range testData {
		if !db2.Has(k) {
			t.Errorf("Key %q not found after reopen", k)
			continue
		}

		rec, err := db2.Get(k)
		if err != nil {
			t.Errorf("Get(%q) error = %v", k, err)
			continue
		}

		if rec.Value != expectedV {
			t.Errorf("Persisted value for %q = %q, want %q", k, rec.Value, expectedV)
		}
	}
}

func TestLograDB_ManyRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping many records test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Insert 1000 records
	numRecords := 1000
	for i := 0; i < numRecords; i++ {
		key := "key-" + itoa(i)
		value := "value-" + itoa(i)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%q) error = %v", key, err)
		}
	}

	// Verify all records
	for i := 0; i < numRecords; i++ {
		key := "key-" + itoa(i)
		expectedValue := "value-" + itoa(i)

		rec, err := db.Get(key)
		if err != nil {
			t.Errorf("Get(%q) error = %v", key, err)
			continue
		}
		if rec.Value != expectedValue {
			t.Errorf("Get(%q).Value = %q, want %q", key, rec.Value, expectedValue)
		}
	}

	db.Close()

	// Reopen and verify persistence
	db2, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer db2.Close()

	for i := 0; i < numRecords; i++ {
		key := "key-" + itoa(i)
		if !db2.Has(key) {
			t.Errorf("Key %q not found after reopen", key)
		}
	}
}
func TestLograDB_haveDirectoryWithoutDatfiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Create directory without any .dat files
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// Try to open database on empty directory
	db, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Verify database is functional
	if err := db.Set("test-key", "test-value"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	if !db.Has("test-key") {
		t.Error("Key should exist after Set")
	}

	rec, err := db.Get("test-key")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}
	if rec.Value != "test-value" {
		t.Errorf("Get().Value = %q, want %q", rec.Value, "test-value")
	}
}

// Simple int to string without importing strconv
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var s string
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}
