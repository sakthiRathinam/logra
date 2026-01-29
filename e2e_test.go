package logra_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sakthirathinam/logra"
	"sakthirathinam/logra/internal/compact"
)

// TestE2E_BasicWorkflow tests the basic create, set, get, close, reopen workflow.
func TestE2E_BasicWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Phase 1: Create and populate
	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	testData := map[string]string{
		"user:1":     "alice",
		"user:2":     "bob",
		"user:3":     "charlie",
		"config:app": "myapp",
		"config:ver": "1.0",
	}

	for k, v := range testData {
		if err := db.Set(k, v); err != nil {
			t.Fatalf("Set(%q) error = %v", k, err)
		}
	}

	// Phase 2: Verify reads work
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

	// Phase 3: Delete some keys
	deleteKeys := []string{"user:2", "config:ver"}
	for _, k := range deleteKeys {
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete(%q) error = %v", k, err)
		}
	}
	db.Close()
	// Phase 3: Reopen and verify persistence and deletions
	db2, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Second Open() error = %v", err)
	}
	defer db2.Close()

	for k, expectedV := range testData {

		if contains(deleteKeys, k) {
			if db2.Has(k) {
				t.Errorf("Key %q should have been deleted", k)
			}
			continue
		}
		if !db2.Has(k) {
			t.Errorf("Key %q not found after reopen", k)
			continue
		}
		rec, err := db2.Get(k)
		if err != nil {
			t.Errorf("Get(%q) after reopen error = %v", k, err)
			continue
		}
		if rec.Value != expectedV {
			t.Errorf("Get(%q).Value after reopen = %q, want %q", k, rec.Value, expectedV)
		}
	}
}

// TestE2E_LargeDataset tests inserting and retrieving a large number of records.
func TestE2E_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E large dataset test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	numRecords := 10000

	// Insert records
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := fmt.Sprintf("value-%06d", i)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%q) error = %v", key, err)
		}
	}

	// Verify all records
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key-%06d", i)
		expectedValue := fmt.Sprintf("value-%06d", i)

		if !db.Has(key) {
			t.Errorf("Key %q not found", key)
			continue
		}

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

	// Verify persistence
	db2, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer db2.Close()

	// Spot check some records
	for _, i := range []int{0, 100, 1000, 5000, 9999} {
		key := fmt.Sprintf("key-%06d", i)
		if !db2.Has(key) {
			t.Errorf("Key %q not found after reopen", key)
		}
	}
}

// TestE2E_UpdateScenarios tests updating the same key multiple times.
func TestE2E_UpdateScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E update test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Update same key multiple times
	key := "counter"
	for i := 1; i <= 100; i++ {
		value := fmt.Sprintf("count-%d", i)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%q) error = %v", key, err)
		}

		// Verify latest value
		rec, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if rec.Value != value {
			t.Errorf("After update %d: Get(%q).Value = %q, want %q", i, key, rec.Value, value)
		}
	}

	// Final verification
	rec, _ := db.Get(key)
	if rec.Value != "count-100" {
		t.Errorf("Final value = %q, want %q", rec.Value, "count-100")
	}
}

// TestE2E_ConcurrentAccess tests that the database can handle sequential access
// from different "sessions" (simulating what concurrent access would look like
// if properly synchronized externally).
// NOTE: LograDB is NOT thread-safe. Concurrent writes require external synchronization.
func TestE2E_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E concurrent test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	numWorkers := 10
	recordsPerWorker := 100

	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*recordsPerWorker)

	// Each worker writes to its own key prefix with mutex protection
	for g := 0; g < numWorkers; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			prefix := fmt.Sprintf("w%d", workerID)

			for i := 0; i < recordsPerWorker; i++ {
				key := fmt.Sprintf("%skey%03d", prefix, i)
				value := fmt.Sprintf("%svalue%03d", prefix, i)

				mu.Lock()
				err := db.Set(key, value)
				mu.Unlock()

				if err != nil {
					errors <- fmt.Errorf("worker %d: Set(%q) error: %v", workerID, key, err)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all records
	for g := 0; g < numWorkers; g++ {
		prefix := fmt.Sprintf("w%d", g)
		for i := 0; i < recordsPerWorker; i++ {
			key := fmt.Sprintf("%skey%03d", prefix, i)
			if !db.Has(key) {
				t.Errorf("Key %q not found", key)
			}
		}
	}
}

// TestE2E_EdgeCases tests edge cases like empty keys, very long values, and unicode.
func TestE2E_EdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E edge cases test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "very long key",
			key:   strings.Repeat("k", 1000),
			value: "value",
		},
		{
			name:  "very long value",
			key:   "longval",
			value: strings.Repeat("v", 100000),
		},
		{
			name:  "unicode key",
			key:   "日本語キー",
			value: "japanese-key-value",
		},
		{
			name:  "unicode value",
			key:   "unicode-val",
			value: "中文值 🎉 emoji ñ é ü",
		},
		{
			name:  "special characters",
			key:   "special!@#$%^&*()",
			value: "value with\ttab\nand newline",
		},
		{
			name:  "binary-like data",
			key:   "binary",
			value: string([]byte{0x00, 0x01, 0x02, 0xFF, 0xFE}),
		},
		// NOTE: Empty values are not supported due to EOF handling in DecodeRecord
	}

	// Write all test cases
	for _, tt := range tests {
		t.Run(tt.name+"-write", func(t *testing.T) {
			if err := db.Set(tt.key, tt.value); err != nil {
				t.Errorf("Set(%q) error = %v", tt.key, err)
			}
		})
	}

	// Read and verify all test cases
	for _, tt := range tests {
		t.Run(tt.name+"-read", func(t *testing.T) {
			rec, err := db.Get(tt.key)
			if err != nil {
				t.Errorf("Get(%q) error = %v", tt.key, err)
				return
			}
			if rec.Value != tt.value {
				t.Errorf("Get(%q).Value mismatch: got len=%d, want len=%d", tt.key, len(rec.Value), len(tt.value))
			}
		})
	}
}

// TestE2E_ReopenManyTimes tests opening and closing the database multiple times.
func TestE2E_ReopenManyTimes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E reopen test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Initial data
	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Initial Open() error = %v", err)
	}
	db.Set("persistent", "data")
	db.Close()

	// Reopen multiple times and verify
	for i := 0; i < 10; i++ {
		db, err := logra.Open(path, "1.0.0")
		if err != nil {
			t.Fatalf("Reopen %d error = %v", i, err)
		}

		if !db.Has("persistent") {
			t.Errorf("Reopen %d: key 'persistent' not found", i)
		}

		// Add a new key
		newKey := fmt.Sprintf("reopen-%d", i)
		db.Set(newKey, "value")

		db.Close()
	}

	// Final verification
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Final Open() error = %v", err)
	}
	defer db.Close()

	if !db.Has("persistent") {
		t.Error("Final: key 'persistent' not found")
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("reopen-%d", i)
		if !db.Has(key) {
			t.Errorf("Final: key %q not found", key)
		}
	}
}

// TestE2E_VersionPersistence verifies that version is properly handled across opens.
func TestE2E_VersionPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E version test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	// Create with version 1
	db1, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("First Open() error = %v", err)
	}
	if db1.Version() != "1.0.0" {
		t.Errorf("Initial Version() = %q, want %q", db1.Version(), "1.0.0")
	}
	db1.Set("key", "value")
	db1.Close()

	// Reopen with version 2
	db2, err := logra.Open(path, "2.0.0")
	if err != nil {
		t.Fatalf("Second Open() error = %v", err)
	}
	defer db2.Close()

	if db2.Version() != "2.0.0" {
		t.Errorf("Reopened Version() = %q, want %q", db2.Version(), "2.0.0")
	}

	// Data should persist
	if !db2.Has("key") {
		t.Error("Key not found after version change")
	}
}

// TestE2E_CompactAfterDeletes tests compaction removes deleted keys and preserves live ones.
func TestE2E_CompactAfterDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E compact test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := db.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Set error = %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := db.Delete(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Delete error = %v", err)
		}
	}
	db.Close()

	// Reopen and compact
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	c := compact.NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	// Reopen and verify
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen after compact error = %v", err)
	}
	defer db.Close()

	for i := 5; i < 10; i++ {
		rec, err := db.Get(fmt.Sprintf("k%d", i))
		if err != nil {
			t.Errorf("Get(k%d) error = %v", i, err)
			continue
		}
		if rec.Value != fmt.Sprintf("v%d", i) {
			t.Errorf("Get(k%d) = %q, want %q", i, rec.Value, fmt.Sprintf("v%d", i))
		}
	}
	for i := 0; i < 5; i++ {
		if db.Has(fmt.Sprintf("k%d", i)) {
			t.Errorf("k%d should have been deleted", i)
		}
	}

	// Reopen once more to verify persistence
	db.Close()
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Final reopen error = %v", err)
	}
	for i := 5; i < 10; i++ {
		if !db.Has(fmt.Sprintf("k%d", i)) {
			t.Errorf("k%d not found after final reopen", i)
		}
	}
}

// TestE2E_CompactAfterUpdates tests compaction keeps latest values for updated keys.
func TestE2E_CompactAfterUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E compact update test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for i := 0; i < 50; i++ {
		db.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("old-%d", i))
	}
	for i := 0; i < 25; i++ {
		db.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("new-%d", i))
	}
	db.Close()

	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	c := compact.NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen after compact error = %v", err)
	}
	defer db.Close()

	for i := 0; i < 25; i++ {
		rec, err := db.Get(fmt.Sprintf("k%d", i))
		if err != nil {
			t.Errorf("Get(k%d) error = %v", i, err)
			continue
		}
		if rec.Value != fmt.Sprintf("new-%d", i) {
			t.Errorf("Get(k%d) = %q, want %q", i, rec.Value, fmt.Sprintf("new-%d", i))
		}
	}
	for i := 25; i < 50; i++ {
		rec, err := db.Get(fmt.Sprintf("k%d", i))
		if err != nil {
			t.Errorf("Get(k%d) error = %v", i, err)
			continue
		}
		if rec.Value != fmt.Sprintf("old-%d", i) {
			t.Errorf("Get(k%d) = %q, want %q", i, rec.Value, fmt.Sprintf("old-%d", i))
		}
	}
}

// TestE2E_CompactWithMultipleFiles tests compaction across multiple dat files.
func TestE2E_CompactWithMultipleFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E compact multi-file test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Write enough to create 3+ dat files (MaxDataFileSize = 1MB)
	bigVal := strings.Repeat("D", 100*1024) // 100KB
	for i := 0; i < 40; i++ {
		db.Set(fmt.Sprintf("big%d", i), bigVal)
	}
	// Delete some
	for i := 0; i < 10; i++ {
		db.Delete(fmt.Sprintf("big%d", i))
	}
	// Update some
	for i := 10; i < 20; i++ {
		db.Set(fmt.Sprintf("big%d", i), bigVal+"updated")
	}
	db.Close()

	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	c := compact.NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen after compact error = %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if db.Has(fmt.Sprintf("big%d", i)) {
			t.Errorf("big%d should be deleted", i)
		}
	}
	for i := 10; i < 20; i++ {
		rec, err := db.Get(fmt.Sprintf("big%d", i))
		if err != nil {
			t.Errorf("Get(big%d) error = %v", i, err)
			continue
		}
		if rec.Value != bigVal+"updated" {
			t.Errorf("big%d has wrong value length %d", i, len(rec.Value))
		}
	}
	for i := 20; i < 40; i++ {
		rec, err := db.Get(fmt.Sprintf("big%d", i))
		if err != nil {
			t.Errorf("Get(big%d) error = %v", i, err)
			continue
		}
		if rec.Value != bigVal {
			t.Errorf("big%d has wrong value length %d", i, len(rec.Value))
		}
	}
}

// TestE2E_WriteDuringCompact simulates writes happening during compaction.
func TestE2E_WriteDuringCompact(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E write-during-compact test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Set initial keys
	for i := 0; i < 10; i++ {
		db.Set(fmt.Sprintf("old%d", i), fmt.Sprintf("oldval%d", i))
	}

	// Run Prepare manually
	c := compact.NewCompact(db)
	if err := c.Prepare(); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	// Write new keys during compaction (goes to file beyond maxFileId)
	for i := 0; i < 5; i++ {
		db.Set(fmt.Sprintf("new%d", i), fmt.Sprintf("newval%d", i))
	}

	// Process each file manually
	for _, f := range c.GetSortedFileObjs() {
		if err := c.ProcessFile(f); err != nil {
			t.Fatalf("processFile error = %v", err)
		}
	}

	// Close merge file, delete old, rename, scan new, swap index
	c.CloseMergeFile()
	c.DeleteOldFiles()
	c.RenameMergeFiles()
	c.ScanNewFiles()
	c.SwapAndCleanup()

	db.Close()

	// Reopen and verify both old and new keys
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		rec, err := db.Get(fmt.Sprintf("old%d", i))
		if err != nil {
			t.Errorf("Get(old%d) error = %v", i, err)
			continue
		}
		if rec.Value != fmt.Sprintf("oldval%d", i) {
			t.Errorf("old%d = %q, want %q", i, rec.Value, fmt.Sprintf("oldval%d", i))
		}
	}
	for i := 0; i < 5; i++ {
		rec, err := db.Get(fmt.Sprintf("new%d", i))
		if err != nil {
			t.Errorf("Get(new%d) error = %v", i, err)
			continue
		}
		if rec.Value != fmt.Sprintf("newval%d", i) {
			t.Errorf("new%d = %q, want %q", i, rec.Value, fmt.Sprintf("newval%d", i))
		}
	}
}

// TestE2E_CompactLargeScale_MultipleFilesInAndOut tests large-scale compaction with many files.
func TestE2E_CompactLargeScale_MultipleFilesInAndOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E large-scale compact test in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Write ~100KB values to create 10+ dat files (MaxDataFileSize = 1MB)
	bigVal := strings.Repeat("V", 100*1024)
	totalKeys := 120
	for i := 0; i < totalKeys; i++ {
		db.Set(fmt.Sprintf("lk%d", i), bigVal+fmt.Sprintf("-%d", i))
	}

	// Update some keys (creates stale data)
	for i := 0; i < 30; i++ {
		db.Set(fmt.Sprintf("lk%d", i), bigVal+fmt.Sprintf("-updated-%d", i))
	}

	// Delete some keys (creates tombstones)
	deleteCount := 20
	for i := 30; i < 30+deleteCount; i++ {
		db.Delete(fmt.Sprintf("lk%d", i))
	}

	db.Close()

	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}

	// Verify 10+ dat files before compact
	filesBefore, _ := db.Storage.GetAllDatFiles()
	for _, f := range filesBefore {
		f.Close()
	}
	if len(filesBefore) < 10 {
		t.Errorf("expected 10+ dat files before compact, got %d", len(filesBefore))
	}

	sizeBefore := dirSizeE2E(t, db.Storage.Dir)

	c := compact.NewCompact(db)
	if err := c.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	db.Close()

	// Reopen and verify
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Reopen after compact error = %v", err)
	}

	// No merge files should remain
	entries, _ := os.ReadDir(db.Storage.Dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "merge") {
			t.Errorf("merge file still exists: %s", e.Name())
		}
	}

	// Verify merge produced 3+ files (enough live data)
	filesAfter, _ := db.Storage.GetAllDatFiles()
	for _, f := range filesAfter {
		f.Close()
	}
	if len(filesAfter) < 3 {
		t.Errorf("expected 3+ dat files after compact, got %d", len(filesAfter))
	}

	// Verify updated keys
	for i := 0; i < 30; i++ {
		rec, err := db.Get(fmt.Sprintf("lk%d", i))
		if err != nil {
			t.Errorf("Get(lk%d) error = %v", i, err)
			continue
		}
		expected := bigVal + fmt.Sprintf("-updated-%d", i)
		if rec.Value != expected {
			t.Errorf("lk%d value mismatch", i)
		}
	}

	// Verify deleted keys are gone
	for i := 30; i < 30+deleteCount; i++ {
		if db.Has(fmt.Sprintf("lk%d", i)) {
			t.Errorf("lk%d should have been deleted", i)
		}
	}

	// Verify remaining live keys
	for i := 30 + deleteCount; i < totalKeys; i++ {
		rec, err := db.Get(fmt.Sprintf("lk%d", i))
		if err != nil {
			t.Errorf("Get(lk%d) error = %v", i, err)
			continue
		}
		expected := bigVal + fmt.Sprintf("-%d", i)
		if rec.Value != expected {
			t.Errorf("lk%d value mismatch", i)
		}
	}

	// Index length = total - deleted
	expectedLive := totalKeys - deleteCount
	if db.Index.Len() != expectedLive {
		t.Errorf("index len = %d, want %d", db.Index.Len(), expectedLive)
	}

	// Disk size should be smaller
	sizeAfter := dirSizeE2E(t, db.Storage.Dir)
	if sizeAfter >= sizeBefore {
		t.Errorf("expected smaller disk after compact: before=%d, after=%d", sizeBefore, sizeAfter)
	}

	db.Close()

	// Final reopen to verify persistence
	db, err = logra.Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("Final reopen error = %v", err)
	}
	defer db.Close()

	for i := 0; i < 30; i++ {
		if !db.Has(fmt.Sprintf("lk%d", i)) {
			t.Errorf("lk%d not found after final reopen", i)
		}
	}
	for i := 30 + deleteCount; i < totalKeys; i++ {
		if !db.Has(fmt.Sprintf("lk%d", i)) {
			t.Errorf("lk%d not found after final reopen", i)
		}
	}
}

func dirSizeE2E(t *testing.T, dir string) int64 {
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

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
