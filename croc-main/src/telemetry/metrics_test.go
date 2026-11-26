package telemetry

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("invalid JSON line %q: %v", scanner.Text(), err)
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func TestEventSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "thư mục có khoảng trắng.jsonl")
	sink, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sink.Emit(ChunkCompress{})
	sink.Emit(StreamStat{})
	sink.Emit(MerkleVerify{})
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	want := map[string][]string{
		"chunk_compress": {"cpu_ns", "entropy", "event", "level", "offset", "raw_bytes", "ts", "wire_bytes"},
		"stream_stat":    {"event", "inflight", "local_port", "stolen_chunks", "throughput_bps", "ts", "worker_id"},
		"merkle_verify":  {"corrupt_chunks", "event", "retransmitted_bytes", "saved_bytes", "ts"},
	}
	lines := readLines(t, path)
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for _, line := range lines {
		name, _ := line["event"].(string)
		var keys []string
		for key := range line {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, want[name]) {
			t.Errorf("%s keys = %v, want %v", name, keys, want[name])
		}
		stamp, _ := line["ts"].(string)
		if _, err := time.Parse(timestampLayout, stamp); err != nil {
			t.Errorf("ts %q: %v", stamp, err)
		}
	}
}

func TestConcurrentEmit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.jsonl")
	sink, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const workers, events = 8, 200
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for range events {
				sink.Emit(StreamStat{WorkerID: worker})
			}
		})
	}
	wg.Wait()
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	counts := map[float64]int{}
	for _, line := range readLines(t, path) {
		counts[line["worker_id"].(float64)]++
	}
	for worker := range workers {
		if counts[float64(worker)] != events {
			t.Errorf("worker %d wrote %d lines, want %d", worker, counts[float64(worker)], events)
		}
	}
}

func TestAppendCloseAndErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.jsonl")
	for range 2 {
		sink, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sink.Emit(MerkleVerify{CorruptChunks: 1})
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		sink.Emit(MerkleVerify{}) // dropped after Close
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(readLines(t, path)); got != 2 {
		t.Fatalf("got %d lines, want 2", got)
	}

	sink, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sink.Emit(StreamStat{ThroughputBps: math.Inf(1)})
	sink.Emit(StreamStat{ThroughputBps: 1})
	if err := sink.Close(); err == nil {
		t.Error("Close must report the encoding error")
	}
	if got := len(readLines(t, path)); got != 3 {
		t.Errorf("got %d lines, want 3: a bad event must not block later ones", got)
	}
}

func TestOpen(t *testing.T) {
	sink, err := Open("")
	if err != nil || sink != Nop {
		t.Fatalf("Open(\"\") = %v, %v; want Nop", sink, err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "missing", "m.jsonl")); err == nil {
		t.Fatal("Open in a missing directory must fail")
	}
}
