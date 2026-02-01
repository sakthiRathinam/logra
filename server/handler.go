package server

import (
	"bufio"
	"strings"

	"sakthirathinam/logra"
)

func HandleCommand(db *logra.LograDB, args []RESPValue, w *bufio.Writer) {
	if len(args) == 0 {
		WriteError(w, "ERR empty command")
		return
	}

	cmd := strings.ToUpper(args[0].Str)

	switch cmd {
	case "PING":
		if len(args) > 1 {
			WriteBulkString(w, args[1].Str)
		} else {
			WriteSimpleString(w, "PONG")
		}

	case "GET":
		if len(args) != 2 {
			WriteError(w, "ERR wrong number of arguments for 'get' command")
			return
		}
		rec, err := db.Get(args[1].Str)
		if err != nil {
			WriteNullBulk(w)
		} else {
			WriteBulkString(w, rec.Value)
		}

	case "SET":
		if len(args) != 3 {
			WriteError(w, "ERR wrong number of arguments for 'set' command")
			return
		}
		err := db.Set(args[1].Str, args[2].Str)
		if err != nil {
			WriteError(w, "ERR "+err.Error())
		} else {
			WriteSimpleString(w, "OK")
		}

	case "DEL":
		if len(args) < 2 {
			WriteError(w, "ERR wrong number of arguments for 'del' command")
			return
		}
		var deleted int64
		for _, arg := range args[1:] {
			if err := db.Delete(arg.Str); err == nil {
				deleted++
			}
		}
		WriteInteger(w, deleted)

	case "EXISTS":
		if len(args) < 2 {
			WriteError(w, "ERR wrong number of arguments for 'exists' command")
			return
		}
		var count int64
		for _, arg := range args[1:] {
			if db.Has(arg.Str) {
				count++
			}
		}
		WriteInteger(w, count)

	case "COMMAND":
		WriteSimpleString(w, "OK")

	case "CONFIG":
		WriteSimpleString(w, "OK")

	case "DBSIZE":
		WriteInteger(w, int64(db.Index.Len()))

	default:
		WriteError(w, "ERR unknown command '"+cmd+"'")
	}
}
