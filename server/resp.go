package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type RESPValue struct {
	Type  byte // '+', '-', ':', '$', '*'
	Str   string
	Int   int64
	Array []RESPValue
}

func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}

func ReadRESP(br *bufio.Reader) (RESPValue, error) {
	typeByte, err := br.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	switch typeByte {
	case '+', '-':
		line, err := readLine(br)
		if err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: typeByte, Str: line}, nil

	case ':':
		line, err := readLine(br)
		if err != nil {
			return RESPValue{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid integer: %w", err)
		}
		return RESPValue{Type: ':', Int: n}, nil

	case '$':
		line, err := readLine(br)
		if err != nil {
			return RESPValue{}, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid bulk length: %w", err)
		}
		if size == -1 {
			return RESPValue{Type: '$', Str: ""}, nil
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: '$', Str: string(buf[:size])}, nil

	case '*':
		line, err := readLine(br)
		if err != nil {
			return RESPValue{}, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid array length: %w", err)
		}
		if count == -1 {
			return RESPValue{Type: '*'}, nil
		}
		arr := make([]RESPValue, count)
		for i := 0; i < count; i++ {
			arr[i], err = ReadRESP(br)
			if err != nil {
				return RESPValue{}, err
			}
		}
		return RESPValue{Type: '*', Array: arr}, nil
	}

	return RESPValue{}, fmt.Errorf("unknown RESP type: %c", typeByte)
}

func WriteSimpleString(w *bufio.Writer, s string) {
	w.WriteByte('+')
	w.WriteString(s)
	w.WriteString("\r\n")
}

func WriteError(w *bufio.Writer, msg string) {
	w.WriteByte('-')
	w.WriteString(msg)
	w.WriteString("\r\n")
}

func WriteBulkString(w *bufio.Writer, s string) {
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(s)))
	w.WriteString("\r\n")
	w.WriteString(s)
	w.WriteString("\r\n")
}

func WriteInteger(w *bufio.Writer, n int64) {
	w.WriteByte(':')
	w.WriteString(strconv.FormatInt(n, 10))
	w.WriteString("\r\n")
}

func WriteNullBulk(w *bufio.Writer) {
	w.WriteString("$-1\r\n")
}

func WriteArray(w *bufio.Writer, count int) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(count))
	w.WriteString("\r\n")
}
