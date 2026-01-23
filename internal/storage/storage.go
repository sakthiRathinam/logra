package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type Storage struct {
	file *os.File
	path string
}

func Open(path string) (*Storage, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Storage{
		file: file,
		path: path,
	}, nil
}

func (s *Storage) Close() error {
	return s.file.Close()
}

func (s *Storage) Append(key, value []byte) (int64, Header, error) {
	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, Header{}, err
	}

	data := EncodeRecord(key, value)
	writer := bufio.NewWriter(s.file)

	if _, err := writer.Write(data); err != nil {
		return 0, Header{}, err
	}
	if err := writer.Flush(); err != nil {
		return 0, Header{}, err
	}

	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return 0, Header{}, err
	}

	return offset, header, nil
}

func (s *Storage) ReadAt(offset int64, header Header) (Record, error) {
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return Record{}, err
	}

	reader := bufio.NewReader(s.file)
	recordSize := HeaderSize + header.KeySize + header.ValueSize
	data := make([]byte, recordSize)

	if _, err := io.ReadFull(reader, data); err != nil {
		return Record{}, err
	}

	return DecodeRecord(data)
}

func (s *Storage) Scan(fn func(offset int64, key []byte, header Header) error) error {
	reader := bufio.NewReader(s.file)
	offset := int64(0)

	for {
		if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		reader.Reset(s.file)

		headerBytes := make([]byte, HeaderSize)
		if _, err := io.ReadFull(reader, headerBytes); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		keySize := binary.LittleEndian.Uint32(headerBytes[4:8])
		valueSize := binary.LittleEndian.Uint32(headerBytes[8:12])

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		header, err := DecodeHeader(headerBytes)
		if err != nil {
			return err
		}

		if err := fn(offset, key, header); err != nil {
			return err
		}

		offset += int64(HeaderSize + keySize + valueSize)
	}
}
