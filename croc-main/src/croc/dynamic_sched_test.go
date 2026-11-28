package croc

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func runLiveRelayTransfer(
	t *testing.T,
	dataPorts int,
	srcFiles []string,
	senderOpts Options,
	receiverOpts Options,
	configureSender func(*Client),
) (receiveDir string) {
	t.Helper()
	relay := startTestRelay(t, dataPorts)
	receiveDir = t.TempDir()
	t.Chdir(receiveDir)

	filesInfo, folders, totalFolders, err := GetFilesInfo(srcFiles, false, false, nil)
	if err != nil {
		t.Fatalf("GetFilesInfo: %v", err)
	}

	secret := fmt.Sprintf("sch04-%d", time.Now().UnixNano())
	for _, o := range []*Options{&senderOpts, &receiverOpts} {
		o.SharedSecret = secret
		o.RelayAddress = relay.address
		o.RelayPassword = "pass123"
		o.DisableLocal = true
		o.DisableClipboard = true
		o.NoPrompt = true
		o.Overwrite = true
		o.Curve = "p256"
	}
	senderOpts.IsSender = true
	senderOpts.Transport = TransportRelay
	senderOpts.RelayPorts = slices.Clone(relay.allPorts)

	s, err := newTestClient(senderOpts)
	if err != nil {
		t.Fatalf("newTestClient sender: %v", err)
	}
	if configureSender != nil {
		configureSender(s)
	}
	r, err := newTestClient(receiverOpts)
	if err != nil {
		t.Fatalf("newTestClient receiver: %v", err)
	}
	t.Cleanup(func() { cancelClients(s, r) })

	results := make(chan error, 2)
	go func() { results <- s.Send(filesInfo, folders, totalFolders) }()
	go func() { results <- r.Receive() }()

	timeout := time.After(60 * time.Second)
	for range 2 {
		select {
		case xferErr := <-results:
			if xferErr != nil {
				t.Fatalf("live transfer failed: %v", xferErr)
			}
		case <-timeout:
			t.Fatal("live transfer timed out")
		}
	}
	return receiveDir
}

func writeDeterministicPayload(t *testing.T, dir, name string, size int, seed int64) (path string, sum [32]byte) {
	t.Helper()
	path = filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(seed))
	hasher := sha256.New()
	buf := make([]byte, 256*1024)
	remaining := size
	for remaining > 0 {
		chunk := min(remaining, len(buf))
		_, _ = rng.Read(buf[:chunk])
		if _, err := f.Write(buf[:chunk]); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		_, _ = hasher.Write(buf[:chunk])
		remaining -= chunk
	}
	copy(sum[:], hasher.Sum(nil))
	return path, sum
}

func sha256File(t *testing.T, path string) [32]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return sha256.Sum256(b)
}

// TestSCH04_100MiBTransferStaticAndDynamicAcross8Streams validates SCH-04's DoD:
// 100+ MiB transfer over 8 parallel TCP streams in both --sched static and
// --sched dynamic modes with SHA-256 verification, netSendDuration callbacks,
// and stream_stat JSONL telemetry.
func TestSCH04_100MiBTransferStaticAndDynamicAcross8Streams(t *testing.T) {
	srcDir := t.TempDir()
	// 100 MiB + 12,345 B exercises both full 32 KiB chunks and the unaligned EOF tail chunk.
	const payloadSize = (100 << 20) + 12345
	srcPath, wantSHA := writeDeterministicPayload(t, srcDir, "payload_100mb.bin", payloadSize, 20261128)

	for _, mode := range []string{SchedModeStatic, SchedModeDynamic} {
		t.Run(mode, func(t *testing.T) {
			metricsPath := filepath.Join(t.TempDir(), "metrics_"+mode+".jsonl")
			var observedCalls atomic.Int64
			var senderRef *Client

			recvDir := runLiveRelayTransfer(
				t,
				8,
				[]string{srcPath},
				Options{
					SchedMode:  mode,
					MetricsOut: metricsPath,
					NoCompress: true,
				},
				Options{
					SchedMode: mode,
				},
				func(s *Client) {
					senderRef = s
					s.SetNetSendObserver(func(d time.Duration) {
						if d >= 0 {
							observedCalls.Add(1)
						}
					})
				},
			)

			gotSHA := sha256File(t, filepath.Join(recvDir, "payload_100mb.bin"))
			if gotSHA != wantSHA {
				t.Fatalf("[%s] SHA-256 mismatch on 100 MiB transfer", mode)
			}
			if observedCalls.Load() == 0 || senderRef.LastNetSendDuration() <= 0 {
				t.Fatalf("[%s] expected positive netSendDuration observations, calls=%d last=%v",
					mode, observedCalls.Load(), senderRef.LastNetSendDuration())
			}

			lines := readStreamStatLines(t, metricsPath)
			if len(lines) < 4 {
				t.Fatalf("[%s] expected stream_stat JSONL lines across parallel workers, got %d", mode, len(lines))
			}
		})
	}
}

func TestSCH04_ThrottledWorkerTriggersWorkStealingOverLiveTCP(t *testing.T) {
	srcDir := t.TempDir()
	const payloadSize = (6 << 20) + 7777 // ~6 MiB (~193 chunks)
	srcPath, wantSHA := writeDeterministicPayload(t, srcDir, "throttled_live.bin", payloadSize, 42)
	metricsPath := filepath.Join(t.TempDir(), "throttled_metrics.jsonl")

	recvDir := runLiveRelayTransfer(
		t,
		4,
		[]string{srcPath},
		Options{
			SchedMode:           SchedModeDynamic,
			DebugThrottleWorker: "0:256k",
			MetricsOut:          metricsPath,
			NoCompress:          true,
		},
		Options{
			SchedMode: SchedModeDynamic,
		},
		nil,
	)

	gotSHA := sha256File(t, filepath.Join(recvDir, "throttled_live.bin"))
	if gotSHA != wantSHA {
		t.Fatalf("SHA-256 mismatch when worker 0 is throttled to 256 KiB/s")
	}

	lines := readStreamStatLines(t, metricsPath)
	if len(lines) == 0 {
		t.Fatalf("expected non-empty stream_stat JSONL")
	}
	var totalStolen uint64
	lastByWorker := make(map[int]parsedStreamStatLine)
	for _, rec := range lines {
		lastByWorker[rec.WorkerID] = rec
	}
	for _, rec := range lastByWorker {
		totalStolen += rec.StolenChunks
	}
	if totalStolen == 0 {
		t.Fatalf("expected work-stealing (stolen_chunks > 0) when worker 0 is throttled to 256 KiB/s")
	}
}

func TestSCH04_MultiFileTransferWithWorkStealing(t *testing.T) {
	srcDir := t.TempDir()
	p1, sha1 := writeDeterministicPayload(t, srcDir, "part1.bin", (2<<20)+333, 101)
	p2, sha2 := writeDeterministicPayload(t, srcDir, "part2.bin", (2<<20)+999, 202)

	recvDir := runLiveRelayTransfer(
		t,
		4,
		[]string{p1, p2},
		Options{
			SchedMode:           SchedModeDynamic,
			DebugThrottleWorker: "1:256k",
			NoCompress:          true,
		},
		Options{
			SchedMode: SchedModeDynamic,
		},
		nil,
	)

	if sha256File(t, filepath.Join(recvDir, "part1.bin")) != sha1 {
		t.Fatalf("SHA-256 mismatch on part1.bin")
	}
	if sha256File(t, filepath.Join(recvDir, "part2.bin")) != sha2 {
		t.Fatalf("SHA-256 mismatch on part2.bin")
	}
}

func TestSCH04_ClientReenqueueRangesAndLegacyFallback(t *testing.T) {
	c, err := New(Options{
		IsSender:     true,
		SharedSecret: "1234-legacy-fallback-check",
		Curve:        "p256",
		SchedMode:    SchedModeDynamic,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Peer has not advertised FeatureDynamicChunkSchedV1 -> must fall back to static!
	if got := c.effectiveSchedMode(); got != SchedModeStatic {
		t.Fatalf("effectiveSchedMode without peer feature = %q, want %q", got, SchedModeStatic)
	}
	c.peerDynamicChunkSched = true
	if got := c.effectiveSchedMode(); got != SchedModeDynamic {
		t.Fatalf("effectiveSchedMode with peer feature = %q, want %q", got, SchedModeDynamic)
	}

	// Verify Client.ReenqueueRanges when no active sender unit exists vs active unit.
	if n := c.ReenqueueRanges([]int64{32768, 0, 2}); n != 0 {
		t.Fatalf("expected 0 enqueued without active sender queue, got %d", n)
	}

	tmpPath := filepath.Join(t.TempDir(), "repair_src.bin")
	if err := os.WriteFile(tmpPath, make([]byte, 32768*4), 0o644); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		t.Fatalf("open temp: %v", err)
	}
	defer f.Close()

	attempt := &transferAttemptState{}
	c.startSenderUnit(attempt, f, 32768*4, false, []int64{32768, 0, 2}, nil)
	if n := c.ReenqueueRanges([]int64{32768, 65536, 2}); n != 2 {
		t.Fatalf("Client.ReenqueueRanges = %d, want 2", n)
	}
	c.joinSenderUnit()

	// Verify unmarkReceivedChunk helper for MRK integration.
	c.receivedChunks = newPagedChunkBitset(32768)
	c.receivedChunks.Set(65536)
	c.unmarkReceivedChunk(65536)
	if c.receivedChunks.Test(65536) {
		t.Fatalf("expected unmarkReceivedChunk to clear offset 65536")
	}
}
