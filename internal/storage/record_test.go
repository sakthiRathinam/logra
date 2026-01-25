package storage

import (
	"bytes"
	"testing"
)

func TestEncodeRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       []byte
		value     []byte
		wantMinLen int
	}{
		{
			name:       "small key-value",
			key:        []byte("key"),
			value:      []byte("value"),
			wantMinLen: HeaderSize + 3 + 5,
		},
		{
			name:       "empty value",
			key:        []byte("key"),
			value:      []byte{},
			wantMinLen: HeaderSize + 3,
		},
		{
			name:       "empty key",
			key:        []byte{},
			value:      []byte("value"),
			wantMinLen: HeaderSize + 5,
		},
		{
			name:       "large value",
			key:        []byte("key"),
			value:      make([]byte, 1000),
			wantMinLen: HeaderSize + 3 + 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EncodeRecord(tt.key, tt.value)

			if len(got) != tt.wantMinLen {
				t.Errorf("EncodeRecord() len = %d, want %d", len(got), tt.wantMinLen)
			}
		})
	}
}

func TestDecodeRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr bool
	}{
		{
			name:    "valid record",
			key:     []byte("testkey"),
			value:   []byte("testvalue"),
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     []byte{},
			value:   []byte("value"),
			wantErr: false,
		},
		{
			name:    "unicode key-value",
			key:     []byte("键"),
			value:   []byte("值"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := EncodeRecord(tt.key, tt.value)
			got, err := DecodeRecord(encoded)

			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !bytes.Equal(got.Key, tt.key) {
				t.Errorf("DecodeRecord() Key = %q, want %q", got.Key, tt.key)
			}
			if !bytes.Equal(got.Value, tt.value) {
				t.Errorf("DecodeRecord() Value = %q, want %q", got.Value, tt.value)
			}
		})
	}
}

func TestDecodeRecord_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "truncated header",
			data:    make([]byte, HeaderSize-1),
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "truncated key",
			data:    func() []byte {
				d := EncodeRecord([]byte("key"), []byte("value"))
				return d[:HeaderSize+1]
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeRecord(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeHeader(t *testing.T) {
	t.Parallel()

	key := []byte("testkey")
	value := []byte("testvalue")
	encoded := EncodeRecord(key, value)

	header, err := DecodeHeader(encoded[:HeaderSize])
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}

	if header.KeySize != uint32(len(key)) {
		t.Errorf("KeySize = %d, want %d", header.KeySize, len(key))
	}
	if header.ValueSize != uint32(len(value)) {
		t.Errorf("ValueSize = %d, want %d", header.ValueSize, len(value))
	}
	if header.CRC == 0 {
		t.Error("CRC should not be zero")
	}
	if header.Timestamp <= 0 {
		t.Error("Timestamp should be positive")
	}
}

func TestDecodeHeader_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "truncated data",
			data:    make([]byte, 4),
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeHeader(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeHeader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "simple strings",
			key:   []byte("hello"),
			value: []byte("world"),
		},
		{
			name:  "binary data",
			key:   []byte{0x00, 0x01, 0x02},
			value: []byte{0xFF, 0xFE, 0xFD},
		},
		{
			name:  "unicode",
			key:   []byte("日本語キー"),
			value: []byte("中文值"),
		},
		{
			name:  "large data",
			key:   bytes.Repeat([]byte("k"), 256),
			value: bytes.Repeat([]byte("v"), 1024*1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := EncodeRecord(tt.key, tt.value)
			decoded, err := DecodeRecord(encoded)

			if err != nil {
				t.Fatalf("DecodeRecord() error = %v", err)
			}

			if !bytes.Equal(decoded.Key, tt.key) {
				t.Errorf("Round trip key mismatch: got %q, want %q", decoded.Key, tt.key)
			}
			if !bytes.Equal(decoded.Value, tt.value) {
				t.Errorf("Round trip value mismatch: got len=%d, want len=%d", len(decoded.Value), len(tt.value))
			}
		})
	}
}

func TestHeader_RecordSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySize  uint32
		valSize  uint32
		wantSize int64
	}{
		{
			name:     "small record",
			keySize:  5,
			valSize:  10,
			wantSize: HeaderSize + 5 + 10,
		},
		{
			name:     "zero sizes",
			keySize:  0,
			valSize:  0,
			wantSize: HeaderSize,
		},
		{
			name:     "large sizes",
			keySize:  1000,
			valSize:  100000,
			wantSize: HeaderSize + 1000 + 100000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := Header{
				KeySize:   tt.keySize,
				ValueSize: tt.valSize,
			}

			if got := h.RecordSize(); got != tt.wantSize {
				t.Errorf("RecordSize() = %d, want %d", got, tt.wantSize)
			}
		})
	}
}
