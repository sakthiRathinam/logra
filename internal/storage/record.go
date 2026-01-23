package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

const HeaderSize = 20

type Header struct {
	CRC       uint32
	Timestamp int64
	KeySize   uint32
	ValueSize uint32
}

type Record struct {
	Header Header
	Key    []byte
	Value  []byte
}

func (h *Header) RecordSize() int64 {
	return int64(HeaderSize + h.KeySize + h.ValueSize)
}

func DecodeHeader(data []byte) (Header, error) {
	var h Header
	buf := bytes.NewReader(data)

	if err := binary.Read(buf, binary.LittleEndian, &h.CRC); err != nil {
		return h, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.KeySize); err != nil {
		return h, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.ValueSize); err != nil {
		return h, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.Timestamp); err != nil {
		if err == io.EOF {
			return h, nil
		}
		return h, err
	}
	return h, nil
}

func DecodeRecord(data []byte) (Record, error) {
	var rec Record
	buf := bytes.NewReader(data)

	if err := binary.Read(buf, binary.LittleEndian, &rec.Header.CRC); err != nil {
		return rec, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &rec.Header.KeySize); err != nil {
		return rec, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &rec.Header.ValueSize); err != nil {
		return rec, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &rec.Header.Timestamp); err != nil {
		return rec, err
	}

	rec.Key = make([]byte, rec.Header.KeySize)
	if _, err := buf.Read(rec.Key); err != nil {
		return rec, err
	}

	rec.Value = make([]byte, rec.Header.ValueSize)
	if _, err := buf.Read(rec.Value); err != nil {
		return rec, err
	}

	return rec, nil
}

func EncodeRecord(key, value []byte) []byte {
	payload := bytes.Buffer{}
	timestamp := time.Now().Unix()

	binary.Write(&payload, binary.LittleEndian, uint32(len(key)))
	binary.Write(&payload, binary.LittleEndian, uint32(len(value)))
	binary.Write(&payload, binary.LittleEndian, int64(timestamp))
	payload.Write(key)
	payload.Write(value)

	crc := crc32.ChecksumIEEE(payload.Bytes())

	final := bytes.Buffer{}
	binary.Write(&final, binary.LittleEndian, crc)
	final.Write(payload.Bytes())

	return final.Bytes()
}
