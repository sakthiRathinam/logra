package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkStorage_Append(b *testing.B) {
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

			s, err := Open(path)
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}
			defer s.Close()

			key := []byte("benchmarkkey")
			value := bytes.Repeat([]byte("v"), size.valueSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Append(key, value)
			}
		})
	}
}

func BenchmarkStorage_ReadAt(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchdb")

	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer s.Close()

	// Write some records
	offsets := make([]int64, 100)
	headers := make([]Header, 100)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := bytes.Repeat([]byte("v"), 1000)
		offsets[i], headers[i], _ = s.Append(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 100
		s.ReadAt(offsets[idx], headers[idx])
	}
}

func BenchmarkStorage_Scan(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("records%d", size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "benchdb")

			s, err := Open(path)
			if err != nil {
				b.Fatalf("Open() error = %v", err)
			}

			for i := 0; i < size; i++ {
				key := []byte(fmt.Sprintf("key%06d", i))
				value := []byte("value")
				s.Append(key, value)
			}
			s.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s, _ = Open(path)
				b.StartTimer()

				s.Scan(func(offset int64, key []byte, header Header, fileID int) error {
					return nil
				})

				b.StopTimer()
				s.Close()
			}
		})
	}
}

func BenchmarkStorage_OpenClose(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "benchdb")

	// Create storage with some data
	s, _ := Open(path)
	for i := 0; i < 100; i++ {
		s.Append([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}
	s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := Open(path)
		s.Close()
	}
}
