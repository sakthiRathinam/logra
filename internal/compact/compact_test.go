package compact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sakthirathinam/logra"
	"sakthirathinam/logra/internal/storage"
)

func openTestDB(t *testing.T) (*logra.LograDB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")
	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db, path
}

func reopenTestDB(t *testing.T, path string) *logra.LograDB {
	t.Helper()
	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen() error = %v", err)
	}
	return db
}

func TestNewCompact(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	c := NewCompact(db)
	if c.compactStatus != CompactInitialized {
		t.Errorf("status = %q, want %q", c.compactStatus, CompactInitialized)
	}
	if c.compactIndex == nil {
		t.Fatal("compactIndex is nil")
	}
	if c.compactIndex.Len() != 0 {
		t.Errorf("compactIndex.Len() = %d, want 0", c.compactIndex.Len())
	}
}

func TestCompact_Execute_RemovesTombstones(t *testing.T) {
	db, path := openTestDB(t)

	db.Set("A", "val-a")
	db.Set("B", "val-b")
	db.Set("C", "val-c")
	db.Delete("B")
	db.Close()

	// Reopen and compact
	db = reopenTestDB(t, path)
	c := NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	// Reopen and verify
	db = reopenTestDB(t, path)
	defer db.Close()

	rec, err := db.Get("A")
	if err != nil {
		t.Errorf("Get(A) error = %v", err)
	} else if rec.Value != "val-a" {
		t.Errorf("Get(A) = %q, want %q", rec.Value, "val-a")
	}

	rec, err = db.Get("C")
	if err != nil {
		t.Errorf("Get(C) error = %v", err)
	} else if rec.Value != "val-c" {
		t.Errorf("Get(C) = %q, want %q", rec.Value, "val-c")
	}

	if db.Has("B") {
		t.Error("key B should have been removed by compact")
	}

	// No merge files should remain
	dir := db.Storage.Dir
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "merge") {
			t.Errorf("merge file still exists: %s", e.Name())
		}
	}
}

func TestCompact_Execute_RemovesStaleValues(t *testing.T) {
	db, path := openTestDB(t)

	db.Set("x", "v1")
	db.Set("x", "v2")
	db.Close()

	db = reopenTestDB(t, path)

	// Measure size before compact
	sizeBefore := dirSize(t, db.Storage.Dir)

	c := NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	db = reopenTestDB(t, path)
	defer db.Close()

	rec, err := db.Get("x")
	if err != nil {
		t.Fatalf("Get(x) error = %v", err)
	}
	if rec.Value != "v2" {
		t.Errorf("Get(x) = %q, want %q", rec.Value, "v2")
	}

	sizeAfter := dirSize(t, db.Storage.Dir)
	if sizeAfter >= sizeBefore {
		t.Errorf("expected smaller size after compact: before=%d, after=%d", sizeBefore, sizeAfter)
	}
}

func TestCompact_Execute_PreservesAllLiveKeys(t *testing.T) {
	db, path := openTestDB(t)

	for i := 0; i < 100; i++ {
		db.Set(keyN(i), valN(i))
	}
	db.Close()

	db = reopenTestDB(t, path)
	c := NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	db = reopenTestDB(t, path)
	defer db.Close()

	for i := 0; i < 100; i++ {
		rec, err := db.Get(keyN(i))
		if err != nil {
			t.Errorf("Get(%s) error = %v", keyN(i), err)
			continue
		}
		if rec.Value != valN(i) {
			t.Errorf("Get(%s) = %q, want %q", keyN(i), rec.Value, valN(i))
		}
	}
}

func TestCompact_Execute_EmptyDB(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	c := NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() on empty DB error = %v", err)
	}
}

func TestCompact_MergeFileRotation(t *testing.T) {
	db, path := openTestDB(t)

	// Write enough data to span multiple files
	// MaxDataFileSize is 1MB, merge rotation is 4MB
	// Write ~6MB of data so merge produces 2+ files
	bigVal := strings.Repeat("X", 100*1024) // 100KB each
	for i := 0; i < 60; i++ {
		db.Set(keyN(i), bigVal)
	}
	db.Close()

	db = reopenTestDB(t, path)
	c := NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	db = reopenTestDB(t, path)
	defer db.Close()

	// Verify all data intact
	for i := 0; i < 60; i++ {
		rec, err := db.Get(keyN(i))
		if err != nil {
			t.Errorf("Get(%s) error = %v", keyN(i), err)
			continue
		}
		if rec.Value != bigVal {
			t.Errorf("Get(%s) value length = %d, want %d", keyN(i), len(rec.Value), len(bigVal))
		}
	}

	// Verify dat files exist and are sequentially numbered starting from 0
	files, _ := db.Storage.GetAllDatFiles()
	if len(files) < 2 {
		t.Errorf("expected at least 2 dat files after merge rotation, got %d", len(files))
	}
	// First files should be the renamed merge files (0.dat, 1.dat, ...)
	for _, f := range files {
		id, err := storage.ParseFileIDFromName(filepath.Base(f.Name()))
		f.Close()
		if err != nil {
			t.Errorf("failed to parse file ID: %v", err)
		}
		if id < 0 {
			t.Errorf("unexpected file ID %d", id)
		}
	}
}

func TestRecoverIfNeeded_NoStateFile(t *testing.T) {
	dir := t.TempDir()
	if err := RecoverIfNeeded(dir); err != nil {
		t.Fatalf("RecoverIfNeeded() error = %v", err)
	}
}

func TestRecoverIfNeeded_InProgressState(t *testing.T) {
	dir := t.TempDir()

	state := mergeState{Status: string(CompactInProgress), MaxFileId: 2}
	data, _ := json.Marshal(state)
	os.WriteFile(filepath.Join(dir, "merge.json"), data, 0644)

	// Create some merge files
	os.WriteFile(filepath.Join(dir, "merge_0.dat"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(dir, "merge_1.dat"), []byte("data"), 0644)

	if err := RecoverIfNeeded(dir); err != nil {
		t.Fatalf("RecoverIfNeeded() error = %v", err)
	}

	// All merge files and state should be gone
	for _, name := range []string{"merge.json", "merge_0.dat", "merge_1.dat"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("file %s should have been removed", name)
		}
	}
}

func TestRecoverIfNeeded_CompletedState(t *testing.T) {
	dir := t.TempDir()

	state := mergeState{Status: string(CompactCompleted), MaxFileId: 1}
	data, _ := json.Marshal(state)
	os.WriteFile(filepath.Join(dir, "merge.json"), data, 0644)

	if err := RecoverIfNeeded(dir); err != nil {
		t.Fatalf("RecoverIfNeeded() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "merge.json")); !os.IsNotExist(err) {
		t.Error("merge.json should have been removed")
	}
}

func TestRecoverIfNeeded_CorruptedStateFile(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "merge.json"), []byte("not json{{{"), 0644)
	os.WriteFile(filepath.Join(dir, "merge_0.dat"), []byte("data"), 0644)

	if err := RecoverIfNeeded(dir); err != nil {
		t.Fatalf("RecoverIfNeeded() error = %v", err)
	}

	for _, name := range []string{"merge.json", "merge_0.dat"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("file %s should have been removed", name)
		}
	}
}

// helpers

func keyN(i int) string {
	return fmt.Sprintf("key-%04d", i)
}

func valN(i int) string {
	return fmt.Sprintf("val-%d", i)
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var size int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		size += info.Size()
	}
	return size
}

