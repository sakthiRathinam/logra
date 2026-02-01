# Logra

A lightweight, embeddable key-value database written in Go. Logra stores data in append-only flat files with an in-memory index for O(1) key lookups, and exposes a RESP-compatible TCP server so any Redis client can talk to it.

## Features

- **Append-only storage** with binary record encoding (CRC32 checksums)
- **In-memory hash index** for O(1) key lookups
- **RESP protocol server** compatible with `redis-cli` and all Redis client libraries
- **Goroutine-safe** with `sync.RWMutex` (concurrent reads, exclusive writes)
- **Automatic file rotation** at 1MB per data file
- **Log compaction** removes tombstones and reclaims disk space
- **Crash recovery** for interrupted compaction

## Installation

```bash
git clone https://github.com/sakthirathinam/logra.git
cd logra
make build
make build-server
```

Requires **Go 1.25.5+**.

## Quick Start

### As a server (recommended)

Start the RESP server:

```bash
# Default: listens on :6379, stores data in ./logra_data
make run-server

# Custom address and data directory
make run-server ADDR=:6380 DB=/var/lib/logra

# Or run the binary directly
./logra-server -addr :6379 -db logra_data
```

Use any Redis client to connect:

```bash
redis-cli SET mykey myvalue    # OK
redis-cli GET mykey            # "myvalue"
redis-cli EXISTS mykey         # (integer) 1
redis-cli DEL mykey            # (integer) 1
redis-cli GET mykey            # (nil)
redis-cli PING                 # PONG
```

### As a CLI

```bash
./logra version
./logra set mykey myvalue
./logra get mykey
./logra del mykey
./logra compact
```

## Supported Commands

| Command | Description |
|---------|-------------|
| `PING` | Returns `PONG` (or echoes argument) |
| `GET key` | Get value by key |
| `SET key value` | Set a key-value pair |
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key [key ...]` | Check if keys exist |
| `DBSIZE` | Return number of keys |

## Architecture

```
┌─────────────────────────────────────────────┐
│              redis-cli / client              │
└──────────────────┬──────────────────────────┘
                   │ RESP over TCP
┌──────────────────▼──────────────────────────┐
│  server/server.go   (goroutine-per-conn)    │
│  server/resp.go     (RESP2 parser)          │
│  server/handler.go  (command dispatch)      │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  db.go              (LograDB)               │
│  ├── sync.RWMutex   (concurrent access)     │
│  ├── Index          (in-memory hash map)    │
│  └── Storage        (append-only files)     │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  0.dat  1.dat  2.dat ...  (data files)      │
│  Record: [CRC|KeySize|ValSize|TS|Key|Value] │
└─────────────────────────────────────────────┘
```

### Record Format

Each record is binary-encoded:

```
Header (20 bytes):
  [CRC32 (4)] [KeySize (4)] [ValueSize (4)] [Timestamp (8)]

Body:
  [Key (KeySize bytes)] [Value (ValueSize bytes)]
```

Deletions are stored as tombstones (`ValueSize = 0`), cleaned up during compaction.

### Compaction

Run `compact` to merge data files, drop tombstones, and reclaim space:

```bash
./logra compact
```

Compaction is crash-safe. If interrupted, it recovers automatically on the next startup.

## Benchmarks

Benchmarked using `redis-benchmark` against the Logra RESP server on the same machine.

**Environment:** Linux, Go 1.25.5, AMD/Intel multi-core

### Throughput (100K requests, 3-byte payload)

| Command | 50 clients | 100 clients | 200 clients |
|---------|-----------|-------------|-------------|
| **PING (inline)** | 86,207 rps | - | - |
| **PING (bulk)** | 87,184 rps | - | - |
| **SET** | 80,000 rps | 76,864 rps | 74,460 rps |
| **GET** | 78,927 rps | 75,358 rps | 74,738 rps |

### Throughput with 256-byte payload

| Command | 50 clients |
|---------|-----------|
| **SET** | 74,627 rps |
| **GET** | 75,188 rps |

### Pipelining (16 commands per batch, 50 clients)

| Command | rps |
|---------|-----|
| **SET** | 85,690 rps |
| **GET** | 282,486 rps |

### Latency (p50)

| Command | 50 clients | 100 clients | 200 clients |
|---------|-----------|-------------|-------------|
| **SET** | 0.319 ms | 0.671 ms | 1.367 ms |
| **GET** | 0.327 ms | 0.663 ms | 1.327 ms |

### Internal Go Benchmarks

| Operation | ops/sec | ns/op | allocs/op |
|-----------|---------|-------|-----------|
| `Has` (index lookup) | 10,772,500 | 103 ns | 1 |
| `Set` (new key) | 203,256 | 5,819 ns | 18 |
| `Get` (100B value) | 119,391 | 9,907 ns | 18 |
| `Get` (1KB value) | 112,873 | 10,469 ns | 18 |
| `Get` (10KB value) | 85,257 | 14,314 ns | 18 |

## Project Structure

```
logra/
├── cmd/
│   ├── logra/              # CLI entry point
│   └── logra-server/       # RESP server entry point
├── server/
│   ├── resp.go             # RESP2 protocol parser/serializer
│   ├── handler.go          # Command dispatch
│   ├── server.go           # TCP listener, goroutine-per-conn
│   ├── resp_test.go
│   └── server_test.go
├── internal/
│   ├── index/              # In-memory hash map index
│   ├── storage/            # Append-only file storage + record encoding
│   └── compact/            # Log compaction
├── db.go                   # LograDB core (Open, Get, Set, Delete, Has)
├── db_test.go
├── db_bench_test.go
├── e2e_test.go
├── Makefile
└── go.mod
```

## Make Targets

```
make build          Build the CLI binary
make build-server   Build the server binary
make run-server     Start RESP server (ADDR=:6379 DB=logra_data)
make run            Run CLI (CMD=version)
make test           Run all tests
make test-unit      Run unit tests only
make test-e2e       Run E2E tests only
make test-race      Run tests with race detection
make bench          Run Go benchmarks
make coverage       Generate coverage report
make fmt            Format code
make vet            Run go vet
make clean          Remove build artifacts
```

## Roadmap

### In Progress

- [ ] **File-level locking (`flock`)** - Cross-process safety using `gofrs/flock` (scaffolded, not yet enabled)

### Planned

- [ ] **Write buffer pool** - Reuse `[]byte` buffers with `sync.Pool` to reduce GC pressure on write-heavy workloads
- [ ] **Batch writes** - Group multiple SET operations into a single fsync for higher throughput
- [ ] **TTL / key expiration** - Support `SET key value EX seconds` and background expiry goroutine
- [ ] **Snapshotting** - Periodic point-in-time snapshots for backup/restore
- [ ] **WAL (Write-Ahead Log)** - Durability guarantees before index update
- [ ] **Connection pooling & pipelining optimization** - Batch flush RESP responses for pipelined commands
- [ ] **MGET / MSET** - Multi-key operations in a single round-trip
- [ ] **Pub/Sub** - Basic publish/subscribe over RESP
- [ ] **Docker support** - Dockerfile and docker-compose for single-command deployment
- [ ] **Prometheus metrics** - Expose ops/sec, latency histograms, connection count
- [ ] **Replication** - Leader-follower replication for read scaling
- [ ] **Range queries** - Ordered index (B-tree or skip list) for key range scans

### Performance Optimization Ideas

- [ ] **mmap reads** - Memory-mapped file reads to skip syscall overhead
- [ ] **io_uring** - Async I/O on Linux for storage operations
- [ ] **Index persistence** - Dump index to disk to avoid full scan on startup
- [ ] **Compaction scheduling** - Automatic background compaction based on tombstone ratio

## Docker (Coming Soon)

```yaml
# docker-compose.yml (planned)
version: "3.8"
services:
  logra:
    build: .
    ports:
      - "6379:6379"
    volumes:
      - logra_data:/data
    command: ["-addr", ":6379", "-db", "/data"]

volumes:
  logra_data:
```

## License

MIT
