package index

type Entry struct {
	Offset    int64
	CRC       uint32
	Timestamp int64
	KeySize   uint32
	ValueSize uint32
	FileID    int
}

type Index struct {
	entries map[string]Entry
}

func New() *Index {
	return &Index{
		entries: make(map[string]Entry),
	}
}

func (idx *Index) Add(key string, entry Entry) {
	idx.entries[key] = entry
}

func (idx *Index) Lookup(key string) (Entry, bool) {
	entry, exists := idx.entries[key]
	return entry, exists
}

func (idx *Index) Has(key string) bool {
	_, exists := idx.entries[key]
	return exists
}

func (idx *Index) Remove(key string) {
	delete(idx.entries, key)
}

func (idx *Index) Keys() []string {
	keys := make([]string, 0, len(idx.entries))
	for k := range idx.entries {
		keys = append(keys, k)
	}
	return keys
}

func (idx *Index) Len() int {
	return len(idx.entries)
}
