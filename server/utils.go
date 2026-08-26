package server

import (
	"bytes"
	"encoding/json"
	"log"

	"github.com/peterouob/Raft/protobuf"
)

func logEntriesToProtobuf(entries []LogEntry) []*protobuf.LogEntry {
	protoEntries := make([]*protobuf.LogEntry, 0, len(entries))
	for i := range entries {
		command, err := json.Marshal(entries[i].Command)
		if err != nil {
			log.Fatal(err)
		}
		protoEntries = append(protoEntries, &protobuf.LogEntry{
			Term:    entries[i].Term,
			Command: command,
		})
	}
	return protoEntries
}

func protobufToLogEntries(entries []*protobuf.LogEntry) []LogEntry {
	logEntries := make([]LogEntry, 0, len(entries))
	for i := range entries {
		var cmd any
		dec := json.NewDecoder(bytes.NewReader(entries[i].Command))
		dec.UseNumber()
		if err := dec.Decode(&cmd); err != nil {
			log.Fatal(err)
		}
		if n, ok := cmd.(json.Number); ok {
			if iv, err := n.Int64(); err == nil {
				cmd = int(iv)
			}
		}
		logEntries = append(logEntries, LogEntry{
			Term:    entries[i].Term,
			Command: cmd,
		})
	}
	return logEntries
}
