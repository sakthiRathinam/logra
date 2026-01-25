package index

import (
	"fmt"
	"testing"
)

func BenchmarkIndex_Add(b *testing.B) {
	idx := New()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		idx.Add(key, Entry{Offset: int64(i * 100)})
	}
}

func BenchmarkIndex_Lookup(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		idx := New()
		for i := 0; i < size; i++ {
			key := fmt.Sprintf("key-%d", i)
			idx.Add(key, Entry{Offset: int64(i * 100)})
		}

		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%d", i%size)
				idx.Lookup(key)
			}
		})
	}
}

func BenchmarkIndex_Has(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		idx := New()
		for i := 0; i < size; i++ {
			key := fmt.Sprintf("key-%d", i)
			idx.Add(key, Entry{Offset: int64(i * 100)})
		}

		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%d", i%size)
				idx.Has(key)
			}
		})
	}
}

func BenchmarkIndex_Remove(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		idx := New()
		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("key-%d", j)
			idx.Add(key, Entry{Offset: int64(j * 100)})
		}
		b.StartTimer()

		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("key-%d", j)
			idx.Remove(key)
		}
	}
}

func BenchmarkIndex_Keys(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		idx := New()
		for i := 0; i < size; i++ {
			key := fmt.Sprintf("key-%d", i)
			idx.Add(key, Entry{Offset: int64(i * 100)})
		}

		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Keys()
			}
		})
	}
}
