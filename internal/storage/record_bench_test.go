package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkEncodeRecord(b *testing.B) {
	sizes := []struct {
		name      string
		keySize   int
		valueSize int
	}{
		{"small10B_100B", 10, 100},
		{"medium100B_1KB", 100, 1024},
		{"large256B_1MB", 256, 1024 * 1024},
	}

	for _, size := range sizes {
		key := bytes.Repeat([]byte("k"), size.keySize)
		value := bytes.Repeat([]byte("v"), size.valueSize)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				EncodeRecord(key, value)
			}
		})
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	sizes := []struct {
		name      string
		keySize   int
		valueSize int
	}{
		{"small10B_100B", 10, 100},
		{"medium100B_1KB", 100, 1024},
		{"large256B_1MB", 256, 1024 * 1024},
	}

	for _, size := range sizes {
		key := bytes.Repeat([]byte("k"), size.keySize)
		value := bytes.Repeat([]byte("v"), size.valueSize)
		encoded := EncodeRecord(key, value)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				DecodeRecord(encoded)
			}
		})
	}
}

func BenchmarkDecodeHeader(b *testing.B) {
	key := []byte("testkey")
	value := []byte("testvalue")
	encoded := EncodeRecord(key, value)
	headerData := encoded[:HeaderSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeHeader(headerData)
	}
}

func BenchmarkHeader_RecordSize(b *testing.B) {
	h := Header{
		KeySize:   100,
		ValueSize: 1000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.RecordSize()
	}
}

func BenchmarkEncodeDecode_RoundTrip(b *testing.B) {
	sizes := []int{100, 1024, 10240}

	for _, size := range sizes {
		key := []byte(fmt.Sprintf("key%d", size))
		value := bytes.Repeat([]byte("v"), size)

		b.Run(fmt.Sprintf("value%dB", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded := EncodeRecord(key, value)
				DecodeRecord(encoded)
			}
		})
	}
}
