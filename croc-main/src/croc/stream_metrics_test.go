package croc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/telemetry"
)

type parsedStreamStatLine struct {
	TS            string  `json:"ts"`
	Event         string  `json:"event"`
	WorkerID      int     `json:"worker_id"`
	LocalPort     int     `json:"local_port"`
	ThroughputBps float64 `json:"throughput_bps"`
	Inflight      int     `json:"inflight"`
	StolenChunks  uint64  `json:"stolen_chunks"`
}

// TestStreamTrackerDetectsOver50PctDropUnder1Second validates the primary DoD of SCH-02:
// detecting a stream whose speed drops by >50% within <1 second.
func TestStreamTrackerDetectsOver50PctDropUnder1Second(t *testing.T) {
	const chunkBytes = 32 << 10 // 32 KiB
	tracker := NewStreamTracker(telemetry.Nop)

	// Warm up 4 streams at ~10.24 MB/s (32 KiB in 3.2 ms).
	fastDur := 3200 * time.Microsecond
	for round := range 4 {
		_ = round
		for id := range 4 {
			tracker.RecordSendSample(id, chunkBytes, fastDur)
		}
	}

	for id := range 4 {
		if tier := tracker.ClassifyWorker(id); tier != WorkerFast {
			t.Fatalf("expected worker %d to be Fast after uniform warm-up, got %s", id, tier)
		}
	}

	// Degrade worker 2 by ~58% (>50% drop): 32 KiB now takes 7.6 ms (~4.3 MB/s),
	// while workers 0, 1, 3 maintain 3.2 ms (~10.24 MB/s).
	degradedDur := 7600 * time.Microsecond
	start := time.Now()
	detected := false

	for time.Since(start) < time.Second {
		for id := range 4 {
			dur := fastDur
			if id == 2 {
				dur = degradedDur
			}
			time.Sleep(dur / 4) // pace simulated wall-clock progression
			tracker.RecordSendSample(id, chunkBytes, dur)
		}
		if tracker.ClassifyWorker(2) == WorkerSlow {
			detected = true
			break
		}
	}

	detectionLatency := time.Since(start)
	if !detected {
		t.Fatalf("failed to classify >50%% degraded worker 2 as Slow within 1s (bps=%.0f, median=%.0f)",
			tracker.WorkerThroughputBps(2), tracker.MedianThroughputBps())
	}
	if detectionLatency >= time.Second {
		t.Fatalf("detection took %v, want < 1s", detectionLatency)
	}
	t.Logf("detected >50%% throughput drop in %v (worker2=%.0f B/s, median=%.0f B/s)",
		detectionLatency, tracker.WorkerThroughputBps(2), tracker.MedianThroughputBps())

	for _, healthyID := range []int{0, 1, 3} {
		if tier := tracker.ClassifyWorker(healthyID); tier != WorkerFast {
			t.Fatalf("healthy worker %d misclassified as %s", healthyID, tier)
		}
	}
}

func TestStreamTrackerDetectsSingleStreamSelfDegradation(t *testing.T) {
	const chunkBytes = 32 << 10
	tracker := NewStreamTracker(telemetry.Nop)

	// Baseline: ~16 MB/s (2 ms per 32 KiB)
	for range 3 {
		tracker.RecordSendSample(0, chunkBytes, 2*time.Millisecond)
	}
	// Drop by 75% to ~4 MB/s (8 ms per 32 KiB)
	for range 5 {
		tracker.RecordSendSample(0, chunkBytes, 8*time.Millisecond)
	}
	if tier := tracker.ClassifyWorker(0); tier != WorkerSlow {
		t.Fatalf("expected self-degraded single worker to be classified Slow, got %s", tier)
	}
}

func TestStreamTrackerDetectsMidSendSocketStallUnder1Second(t *testing.T) {
	const chunkBytes = 32 << 10
	tracker := NewStreamTracker(telemetry.Nop)

	// Workers 0 and 1 warm up at ~16 MB/s (2 ms per chunk).
	for range 3 {
		tracker.RecordSendSample(0, chunkBytes, 2*time.Millisecond)
		tracker.RecordSendSample(1, chunkBytes, 2*time.Millisecond)
	}
	if tracker.ClassifyWorker(1) != WorkerFast {
		t.Fatalf("expected worker 1 to start Fast")
	}

	// Worker 1 begins a socket write and stalls mid-flight without returning!
	start := time.Now()
	tracker.MarkSendStart(1, chunkBytes)
	time.Sleep(45 * time.Millisecond)

	if tier := tracker.ClassifyWorker(1); tier != WorkerSlow {
		t.Fatalf("expected stalled in-flight worker 1 to become Slow in <1s, got %s (bps=%.0f)",
			tier, tracker.WorkerThroughputBps(1))
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("stall detection took %v, want <1s", elapsed)
	}

	tracker.MarkSendFinish(1)
}

func TestStreamTrackerRegisterWorkerConnExtractsTCPPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- c
		}
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()
	serverConn := <-accepted
	defer serverConn.Close()

	wantPort := clientConn.LocalAddr().(*net.TCPAddr).Port
	dataConn := comm.New(clientConn)

	jsonlPath := filepath.Join(t.TempDir(), "port_check.jsonl")
	sink, err := telemetry.Open(jsonlPath)
	if err != nil {
		t.Fatalf("telemetry.Open: %v", err)
	}

	tracker := NewStreamTracker(sink)
	tracker.RegisterWorkerConn(0, dataConn)
	tracker.RecordSendSample(0, 32768, 4*time.Millisecond)
	tracker.Stop()
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	lines := readStreamStatLines(t, jsonlPath)
	if len(lines) != 1 {
		t.Fatalf("expected 1 stream_stat line, got %d", len(lines))
	}
	if lines[0].LocalPort != wantPort {
		t.Fatalf("LocalPort = %d, want %d", lines[0].LocalPort, wantPort)
	}
}

func TestStreamTrackerPeriodic200msJSONLTelemetry(t *testing.T) {
	jsonlPath := filepath.Join(t.TempDir(), "stream_metrics.jsonl")
	sink, err := telemetry.Open(jsonlPath)
	if err != nil {
		t.Fatalf("telemetry.Open: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use exact 200 ms default interval from SCH-02 spec.
	tracker := NewStreamTracker(sink)
	tracker.RegisterWorker(0, 9009)
	tracker.RegisterWorker(1, 9010)
	tracker.RecordSendSample(0, 32768, 2*time.Millisecond)
	tracker.RecordSendSample(1, 32768, 20*time.Millisecond)
	tracker.RecordSteal(0)
	tracker.MarkSendStart(1, 32768)

	tracker.Start(ctx)
	// Let at least 2 periodic 200ms ticks fire, then Stop() flushes final snapshot.
	time.Sleep(450 * time.Millisecond)
	tracker.MarkSendFinish(1)
	tracker.Stop()

	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	lines := readStreamStatLines(t, jsonlPath)
	// 2 workers * (>=2 periodic ticks + 1 final Stop flush) >= 6 lines.
	if len(lines) < 6 {
		t.Fatalf("expected >= 6 JSONL stream_stat records (200ms cadence + final flush), got %d", len(lines))
	}

	sawInflightOnWorker1 := false
	sawStolenOnWorker0 := false
	for i, rec := range lines {
		if rec.Event != "stream_stat" {
			t.Fatalf("line %d: event = %q, want stream_stat", i, rec.Event)
		}
		if _, err := time.Parse(time.RFC3339Nano, rec.TS); err != nil {
			t.Fatalf("line %d: invalid timestamp %q: %v", i, rec.TS, err)
		}
		if rec.WorkerID == 0 {
			if rec.LocalPort != 9009 {
				t.Fatalf("worker 0 port = %d, want 9009", rec.LocalPort)
			}
			if rec.StolenChunks == 1 {
				sawStolenOnWorker0 = true
			}
		} else if rec.WorkerID == 1 {
			if rec.LocalPort != 9010 {
				t.Fatalf("worker 1 port = %d, want 9010", rec.LocalPort)
			}
			if rec.Inflight == 1 {
				sawInflightOnWorker1 = true
			}
		}
	}
	if !sawInflightOnWorker1 {
		t.Fatalf("expected periodic telemetry to capture inflight=1 on worker 1")
	}
	if !sawStolenOnWorker0 {
		t.Fatalf("expected telemetry to capture stolen_chunks=1 on worker 0")
	}
}

// TestSCH01_02_03_EndToEndSchedulerAndTelemetryIntegration verifies that
// SCH-01 (ChunkScheduler), SCH-02 (StreamTracker + telemetry.Sink), and
// SCH-03 (adaptiveChunkScheduler work-stealing) work seamlessly together.
func TestSCH01_02_03_EndToEndSchedulerAndTelemetryIntegration(t *testing.T) {
	const (
		chunkSize   = int64(32 << 10)
		totalChunks = 48
	)
	jsonlPath := filepath.Join(t.TempDir(), "sch_integrated.jsonl")
	sink, err := telemetry.Open(jsonlPath)
	if err != nil {
		t.Fatalf("telemetry.Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tracker := NewStreamTracker(sink, StreamTrackerConfig{
		TelemetryInterval: 25 * time.Millisecond,
	})
	for id := range 4 {
		tracker.RegisterWorker(id, 9009+id)
	}
	tracker.Start(ctx)

	var doneCalls atomic.Int32
	sched := newAdaptiveChunkScheduler(nil, chunkSize*totalChunks, chunkSize, tracker, func() {
		doneCalls.Add(1)
		cancel()
	})

	var firstCompletions atomic.Int32
	var wg sync.WaitGroup
	for workerID := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			delay := 2 * time.Millisecond
			if id == 1 {
				delay = 25 * time.Millisecond // port 9010 throttled >10x
			}
			for {
				tk, ok := sched.Acquire(ctx, id)
				if !ok {
					return
				}
				tracker.MarkSendStart(id, int(chunkSize))
				t0 := time.Now()
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					tracker.MarkSendFinish(id)
					return
				}
				sendDur := time.Since(t0)
				tracker.MarkSendFinish(id)
				if sched.Complete(id, tk, int(chunkSize), sendDur) {
					firstCompletions.Add(1)
				}
			}
		}(workerID)
	}
	wg.Wait()
	tracker.Stop()
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	st := sched.Stats()
	if st.CompletedChunks != totalChunks || firstCompletions.Load() != totalChunks {
		t.Fatalf("incomplete transfer: stats=%+v firstCompletions=%d", st, firstCompletions.Load())
	}
	if doneCalls.Load() != 1 {
		t.Fatalf("onDone called %d times, want 1", doneCalls.Load())
	}
	if st.StolenChunks == 0 {
		t.Fatalf("expected work-stealing to rescue chunks from throttled worker 1")
	}

	lines := readStreamStatLines(t, jsonlPath)
	if len(lines) < 4 {
		t.Fatalf("expected >= 4 JSONL stream_stat lines, got %d", len(lines))
	}
	var totalStolenInJSONL uint64
	lastByWorker := make(map[int]parsedStreamStatLine)
	for _, l := range lines {
		lastByWorker[l.WorkerID] = l
	}
	for _, l := range lastByWorker {
		totalStolenInJSONL += l.StolenChunks
	}
	if totalStolenInJSONL != st.StolenChunks {
		t.Fatalf("JSONL stolen_chunks sum = %d, want %d", totalStolenInJSONL, st.StolenChunks)
	}
	if lastByWorker[1].ThroughputBps >= lastByWorker[0].ThroughputBps*0.5 {
		t.Fatalf("expected throttled worker 1 bps (%.0f) < 50%% of fast worker 0 bps (%.0f)",
			lastByWorker[1].ThroughputBps, lastByWorker[0].ThroughputBps)
	}
}

func readStreamStatLines(t *testing.T, path string) []parsedStreamStatLine {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var out []parsedStreamStatLine
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec parsedStreamStatLine
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			t.Fatalf("unmarshal JSONL line %q: %v", scanner.Text(), err)
		}
		out = append(out, rec)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}
