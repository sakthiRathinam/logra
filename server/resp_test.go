package server

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReadRESPSimpleString(t *testing.T) {
	input := "+OK\r\n"
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	val, err := ReadRESP(br)
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '+' || val.Str != "OK" {
		t.Fatalf("expected +OK, got %c %q", val.Type, val.Str)
	}
}

func TestReadRESPError(t *testing.T) {
	input := "-ERR unknown\r\n"
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	val, err := ReadRESP(br)
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '-' || val.Str != "ERR unknown" {
		t.Fatalf("expected error, got %c %q", val.Type, val.Str)
	}
}

func TestReadRESPInteger(t *testing.T) {
	input := ":42\r\n"
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	val, err := ReadRESP(br)
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != ':' || val.Int != 42 {
		t.Fatalf("expected :42, got %c %d", val.Type, val.Int)
	}
}

func TestReadRESPBulkString(t *testing.T) {
	input := "$5\r\nhello\r\n"
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	val, err := ReadRESP(br)
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '$' || val.Str != "hello" {
		t.Fatalf("expected $hello, got %c %q", val.Type, val.Str)
	}
}

func TestReadRESPArray(t *testing.T) {
	input := "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	val, err := ReadRESP(br)
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '*' || len(val.Array) != 2 {
		t.Fatalf("expected array of 2, got %c len=%d", val.Type, len(val.Array))
	}
	if val.Array[0].Str != "GET" || val.Array[1].Str != "foo" {
		t.Fatalf("unexpected array contents")
	}
}

func TestWriteSimpleString(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteSimpleString(w, "OK")
	w.Flush()
	if buf.String() != "+OK\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteBulkString(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteBulkString(w, "hello")
	w.Flush()
	if buf.String() != "$5\r\nhello\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteNullBulk(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteNullBulk(w)
	w.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteInteger(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteInteger(w, 100)
	w.Flush()
	if buf.String() != ":100\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteError(w, "ERR bad")
	w.Flush()
	if buf.String() != "-ERR bad\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}
