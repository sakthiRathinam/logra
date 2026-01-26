package logra

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkLograDB_Set(b *testing.B) {
	b.Run("newkeys", func(b *testing.B) {
		dir := b.TempDir()
		path := filepath.Join(dir, "benchdb")
		db, err := Open(path, "1.0.0")
		if err != nil {
			b.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			db.Set(key, "testvalue")
		}
	})

	b.Run("updatesamekey", func(b *testing.B) {
		dir := b.TempDir()
		path := filepath.Join(dir, "benchdb")

		db, err := Open(path, "1.0.0")
		if err != nil {
			b.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Set("samekey", "testvalue")
		}
	})
}

func BenchmarkLograDB_Get(b *testing.B) {
	sizes := []struct {
		name      string
		valueSize int
	}{
		{"small100B", 100},
		{"medium1KB", 1024},
		{"large10KB", 10240},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "benchdb")

			db, err := Open(path, "1.0.0")
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			defer db.Close()

			// Pre-populate
			value := strings.Repeat("v", size.valueSize)
			for i := 0; i < 100; i++ {
				db.Set(fmt.Sprintf("key%d", i), value)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%d", i%100)
				db.Get(key)
			}
		})
	}
}

func BenchmarkLograDB_Has(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		db.Has(key)
	}
}

func BenchmarkLograDB_Open(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("records%d", size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "benchdb")

			// Create database with records
			db, err := Open(path, "1.0.0")
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			for i := 0; i < size; i++ {
				db.Set(fmt.Sprintf("key%06d", i), "value")
			}
			db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db, _ := Open(path, "1.0.0")
				db.Close()
			}
		})
	}
}

func BenchmarkLograDB_SetGet(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchdb")

	db, err := Open(path, "1.0.0")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		db.Set(key, "testvalue")
		db.Get(key)
	}
}
