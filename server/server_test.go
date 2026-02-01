package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"testing"

	"sakthirathinam/logra"
)

func setupTestServer(t *testing.T) (*Server, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	db, err := logra.Open(dir, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}

	srv, err := New(db, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		conn.Close()
		srv.Close()
		db.Close()
	})

	return srv, conn
}

func sendCommand(conn net.Conn, args ...string) (RESPValue, error) {
	bw := bufio.NewWriter(conn)
	WriteArray(bw, len(args))
	for _, a := range args {
		WriteBulkString(bw, a)
	}
	bw.Flush()

	br := bufio.NewReader(conn)
	return ReadRESP(br)
}

func TestPing(t *testing.T) {
	_, conn := setupTestServer(t)
	val, err := sendCommand(conn, "PING")
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '+' || val.Str != "PONG" {
		t.Fatalf("expected PONG, got %c %q", val.Type, val.Str)
	}
}

func TestSetAndGet(t *testing.T) {
	_, conn := setupTestServer(t)

	val, err := sendCommand(conn, "SET", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "OK" {
		t.Fatalf("expected OK, got %q", val.Str)
	}

	val, err = sendCommand(conn, "GET", "foo")
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "bar" {
		t.Fatalf("expected bar, got %q", val.Str)
	}
}

func TestGetMissing(t *testing.T) {
	_, conn := setupTestServer(t)
	val, err := sendCommand(conn, "GET", "nope")
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '$' || val.Str != "" {
		t.Fatalf("expected null bulk, got %c %q", val.Type, val.Str)
	}
}

func TestDeleteAndExists(t *testing.T) {
	_, conn := setupTestServer(t)

	sendCommand(conn, "SET", "key1", "val1")

	val, err := sendCommand(conn, "EXISTS", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if val.Int != 1 {
		t.Fatalf("expected 1, got %d", val.Int)
	}

	val, err = sendCommand(conn, "DEL", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if val.Int != 1 {
		t.Fatalf("expected 1 deleted, got %d", val.Int)
	}

	val, err = sendCommand(conn, "EXISTS", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if val.Int != 0 {
		t.Fatalf("expected 0, got %d", val.Int)
	}
}

func TestUnknownCommand(t *testing.T) {
	_, conn := setupTestServer(t)
	val, err := sendCommand(conn, "FLUSHALL")
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != '-' {
		t.Fatalf("expected error, got %c", val.Type)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	fmt.Println()
	os.Exit(code)
}
