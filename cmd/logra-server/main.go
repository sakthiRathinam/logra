package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sakthirathinam/logra"
	"sakthirathinam/logra/server"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address")
	dbPath := flag.String("db", "logra_data", "database directory path")
	flag.Parse()

	db, err := logra.Open(*dbPath, "1.0.0")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	srv, err := server.New(db, *addr)
	if err != nil {
		db.Close()
		log.Fatalf("failed to start server: %v", err)
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nShutting down...")
		srv.Close()
		db.Close()
		os.Exit(0)
	}()

	if err := srv.Serve(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
