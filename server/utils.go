package server

import (
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
		logEntries = append(logEntries, LogEntry{
			Term:    entries[i].Term,
			Command: json.RawMessage(entries[i].Command),
		})
	}
	return logEntries
}
