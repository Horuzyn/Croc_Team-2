// Package telemetry writes transfer metrics as JSON Lines (--metrics-out).
//
// Every line is one JSON object that starts with "ts" (UTC, microseconds) and
// "event", followed by the fields of the event type.
package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const timestampLayout = "2006-01-02T15:04:05.000000Z07:00"

// Event is one telemetry record. EventName becomes the "event" field.
type Event interface {
	EventName() string
}

// ChunkCompress describes the compression decision for one chunk (CMP).
type ChunkCompress struct {
	Offset    int64   `json:"offset"`
	Entropy   float64 `json:"entropy"` // estimated bits per byte
	Level     int     `json:"level"`   // flate level used; see CMP for raw chunks
	RawBytes  int     `json:"raw_bytes"`
	WireBytes int     `json:"wire_bytes"`
	CPUNanos  int64   `json:"cpu_ns"`
}

// EventName implements Event.
func (ChunkCompress) EventName() string { return "chunk_compress" }

// StreamStat is a periodic sample of one data stream (SCH).
type StreamStat struct {
	WorkerID      int     `json:"worker_id"`
	LocalPort     int     `json:"local_port"`
	ThroughputBps float64 `json:"throughput_bps"` // bytes per second, not bits
	Inflight      int     `json:"inflight"`
	StolenChunks  uint64  `json:"stolen_chunks"`
}

// EventName implements Event.
func (StreamStat) EventName() string { return "stream_stat" }

// MerkleVerify summarises chunk verification and repair (MRK).
type MerkleVerify struct {
	CorruptChunks      int   `json:"corrupt_chunks"`
	RetransmittedBytes int64 `json:"retransmitted_bytes"`
	SavedBytes         int64 `json:"saved_bytes"`
}

// EventName implements Event.
func (MerkleVerify) EventName() string { return "merkle_verify" }

// Sink receives events. Implementations are safe for concurrent use.
//
// Emit never fails a transfer: the first error is kept and returned by Close.
type Sink interface {
	Emit(Event)
	Close() error
}

// Nop discards every event. It is the sink used without --metrics-out.
var Nop Sink = nopSink{}

type nopSink struct{}

func (nopSink) Emit(Event)   {}
func (nopSink) Close() error { return nil }

type fileSink struct {
	mu     sync.Mutex
	file   *os.File // nil once closed
	err    error
	closed error
}

// Open returns a sink that appends to path, or Nop when path is empty.
//
// Each event is written with a single unbuffered write, so lines already
// emitted survive an interrupted transfer.
func Open(path string) (Sink, error) {
	if path == "" {
		return Nop, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open metrics file: %w", err)
	}
	return &fileSink{file: file}, nil
}

// Emit appends one line. Events emitted after Close are dropped.
func (s *fileSink) Emit(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return
	}
	line, err := encode(time.Now(), event)
	if err == nil {
		_, err = s.file.Write(line)
	}
	if err != nil && s.err == nil {
		s.err = err
	}
}

// Close closes the file. It is safe to call more than once.
func (s *fileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		s.closed = errors.Join(s.err, s.file.Close())
		s.file = nil
	}
	return s.closed
}

func encode(now time.Time, event Event) ([]byte, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode %s event: %w", event.EventName(), err)
	}
	header, err := json.Marshal(struct {
		Timestamp string `json:"ts"`
		Event     string `json:"event"`
	}{now.UTC().Format(timestampLayout), event.EventName()})
	if err != nil {
		return nil, err
	}
	if len(body) < 2 || body[0] != '{' {
		return nil, fmt.Errorf("%s event is not a JSON object", event.EventName())
	}
	line := header[:len(header)-1]
	if len(body) > 2 {
		line = append(line, ',')
	}
	line = append(line, body[1:]...)
	return append(line, '\n'), nil
}
