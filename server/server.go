package server

import (
	"bufio"
	"log"
	"net"

	"sakthirathinam/logra"
)

type Server struct {
	db       *logra.LograDB
	listener net.Listener
}

func New(db *logra.LograDB, addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	log.Printf("Logra server listening on %s", addr)
	return &Server{db: db, listener: ln}, nil
}

func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		val, err := ReadRESP(br)
		if err != nil {
			return
		}

		if val.Type == '*' {
			HandleCommand(s.db, val.Array, bw)
		} else {
			WriteError(bw, "ERR expected array")
		}
		bw.Flush()
	}
}

func (s *Server) Close() error {
	return s.listener.Close()
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}
