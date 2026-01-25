package index

import (
	"sort"
	"testing"
)

func TestNew(t *testing.T) {
	t.Parallel()

	idx := New()
	if idx == nil {
		t.Fatal("New() returned nil")
	}
	if idx.Len() != 0 {
		t.Errorf("New() index length = %d, want 0", idx.Len())
	}
}

func TestIndex_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries map[string]Entry
		wantLen int
	}{
		{
			name:    "single entry",
			entries: map[string]Entry{"key1": {Offset: 100, CRC: 12345}},
			wantLen: 1,
		},
		{
			name: "multiple entries",
			entries: map[string]Entry{
				"key1": {Offset: 100},
				"key2": {Offset: 200},
				"key3": {Offset: 300},
			},
			wantLen: 3,
		},
		{
			name: "overwrite same key",
			entries: map[string]Entry{
				"key1": {Offset: 100},
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx := New()

			for k, v := range tt.entries {
				idx.Add(k, v)
			}

			if idx.Len() != tt.wantLen {
				t.Errorf("Len() = %d, want %d", idx.Len(), tt.wantLen)
			}
		})
	}
}

func TestIndex_Add_Overwrite(t *testing.T) {
	t.Parallel()

	idx := New()
	idx.Add("key1", Entry{Offset: 100})
	idx.Add("key1", Entry{Offset: 200})

	entry, exists := idx.Lookup("key1")
	if !exists {
		t.Fatal("key1 should exist")
	}
	if entry.Offset != 200 {
		t.Errorf("Offset = %d, want 200 (overwritten value)", entry.Offset)
	}
	if idx.Len() != 1 {
		t.Errorf("Len() = %d, want 1 after overwrite", idx.Len())
	}
}

func TestIndex_Lookup(t *testing.T) {
	t.Parallel()

	idx := New()
	entry := Entry{
		Offset:    100,
		CRC:       12345,
		Timestamp: 1234567890,
		KeySize:   5,
		ValueSize: 10,
		FileID:    0,
	}
	idx.Add("existing", entry)

	tests := []struct {
		name       string
		key        string
		wantExists bool
		wantEntry  Entry
	}{
		{
			name:       "existing key",
			key:        "existing",
			wantExists: true,
			wantEntry:  entry,
		},
		{
			name:       "missing key",
			key:        "missing",
			wantExists: false,
			wantEntry:  Entry{},
		},
		{
			name:       "empty key",
			key:        "",
			wantExists: false,
			wantEntry:  Entry{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, exists := idx.Lookup(tt.key)

			if exists != tt.wantExists {
				t.Errorf("Lookup(%q) exists = %v, want %v", tt.key, exists, tt.wantExists)
			}
			if exists && got != tt.wantEntry {
				t.Errorf("Lookup(%q) = %+v, want %+v", tt.key, got, tt.wantEntry)
			}
		})
	}
}

func TestIndex_Has(t *testing.T) {
	t.Parallel()

	idx := New()
	idx.Add("existing", Entry{Offset: 100})

	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"existing key", "existing", true},
		{"missing key", "missing", false},
		{"empty key", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := idx.Has(tt.key); got != tt.want {
				t.Errorf("Has(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestIndex_Remove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialKeys   []string
		removeKey     string
		wantLen       int
		wantHasRemove bool
	}{
		{
			name:          "remove existing key",
			initialKeys:   []string{"key1", "key2", "key3"},
			removeKey:     "key2",
			wantLen:       2,
			wantHasRemove: false,
		},
		{
			name:          "remove non-existing key",
			initialKeys:   []string{"key1", "key2"},
			removeKey:     "missing",
			wantLen:       2,
			wantHasRemove: false,
		},
		{
			name:          "remove from empty index",
			initialKeys:   []string{},
			removeKey:     "key1",
			wantLen:       0,
			wantHasRemove: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx := New()
			for i, k := range tt.initialKeys {
				idx.Add(k, Entry{Offset: int64(i * 100)})
			}

			idx.Remove(tt.removeKey)

			if idx.Len() != tt.wantLen {
				t.Errorf("Len() after Remove = %d, want %d", idx.Len(), tt.wantLen)
			}
			if idx.Has(tt.removeKey) != tt.wantHasRemove {
				t.Errorf("Has(%q) after Remove = %v, want %v", tt.removeKey, idx.Has(tt.removeKey), tt.wantHasRemove)
			}
		})
	}
}

func TestIndex_Keys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keys     []string
		wantKeys []string
	}{
		{
			name:     "empty index",
			keys:     []string{},
			wantKeys: []string{},
		},
		{
			name:     "single key",
			keys:     []string{"key1"},
			wantKeys: []string{"key1"},
		},
		{
			name:     "multiple keys",
			keys:     []string{"key1", "key2", "key3"},
			wantKeys: []string{"key1", "key2", "key3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx := New()
			for i, k := range tt.keys {
				idx.Add(k, Entry{Offset: int64(i * 100)})
			}

			got := idx.Keys()
			sort.Strings(got)
			sort.Strings(tt.wantKeys)

			if len(got) != len(tt.wantKeys) {
				t.Errorf("Keys() length = %d, want %d", len(got), len(tt.wantKeys))
			}
			for i := range got {
				if got[i] != tt.wantKeys[i] {
					t.Errorf("Keys()[%d] = %q, want %q", i, got[i], tt.wantKeys[i])
				}
			}
		})
	}
}

func TestIndex_Len(t *testing.T) {
	t.Parallel()

	idx := New()

	if idx.Len() != 0 {
		t.Errorf("Len() initial = %d, want 0", idx.Len())
	}

	idx.Add("key1", Entry{Offset: 100})
	if idx.Len() != 1 {
		t.Errorf("Len() after 1 add = %d, want 1", idx.Len())
	}

	idx.Add("key2", Entry{Offset: 200})
	idx.Add("key3", Entry{Offset: 300})
	if idx.Len() != 3 {
		t.Errorf("Len() after 3 adds = %d, want 3", idx.Len())
	}

	idx.Remove("key2")
	if idx.Len() != 2 {
		t.Errorf("Len() after remove = %d, want 2", idx.Len())
	}
}
