package logra

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// setupTestDB creates a new database in a temporary directory and returns
// the database instance along with a cleanup function.
func setupTestDB(t *testing.T) (*LograDB, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		t.Fatalf("setupTestDB: Open() error = %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// setupTestDBWithVersion creates a new database with a specific version.
func setupTestDBWithVersion(t *testing.T, version string) (*LograDB, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, version)
	if err != nil {
		t.Fatalf("setupTestDBWithVersion: Open() error = %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// generateTestKey generates a test key with the given prefix and index.
func generateTestKey(prefix string, i int) string {
	return fmt.Sprintf("%s-%06d", prefix, i)
}

// generateTestValue generates a test value of the specified size.
func generateTestValue(size int) string {
	return strings.Repeat("v", size)
}

// assertNoError fails the test if err is not nil.
func assertNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", msg, err)
	}
}

// assertError fails the test if err is nil.
func assertError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error but got nil", msg)
	}
}

// assertEqual fails the test if got != want.
func assertEqual[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}

// assertTrue fails the test if condition is false.
func assertTrue(t *testing.T, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Errorf("%s: expected true but got false", msg)
	}
}

// assertFalse fails the test if condition is true.
func assertFalse(t *testing.T, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Errorf("%s: expected false but got true", msg)
	}
}

// populateDB adds n records to the database with the given key prefix.
func populateDB(t *testing.T, db *LograDB, n int, keyPrefix string) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := generateTestKey(keyPrefix, i)
		value := generateTestValue(100)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("populateDB: Set(%q) error = %v", key, err)
		}
	}
}
