package croc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/crypt"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
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
	receiveDir, _, _, _ = runLiveRelayTransferWithClients(t, dataPorts, srcFiles, senderOpts, receiverOpts, configureSender)
	return receiveDir
}

func runLiveRelayTransferWithClients(
	t *testing.T,
	dataPorts int,
	srcFiles []string,
	senderOpts Options,
	receiverOpts Options,
	configureSender func(*Client),
) (receiveDir string, sender *Client, receiver *Client, elapsed time.Duration) {
	t.Helper()
	relay := startTestRelay(t, dataPorts)
	receiveDir = t.TempDir()
	t.Chdir(receiveDir)

	filesInfo, folders, totalFolders, err := GetFilesInfo(srcFiles, false, false, nil)
	if err != nil {
		t.Fatalf("GetFilesInfo: %v", err)
	}

	secret := fmt.Sprintf("sch-%d", time.Now().UnixNano())
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

	sender, err = newTestClient(senderOpts)
	if err != nil {
		t.Fatalf("newTestClient sender: %v", err)
	}
	if configureSender != nil {
		configureSender(sender)
	}
	receiver, err = newTestClient(receiverOpts)
	if err != nil {
		t.Fatalf("newTestClient receiver: %v", err)
	}
	t.Cleanup(func() { cancelClients(sender, receiver) })

	start := time.Now()
	results := make(chan error, 2)
	go func() { results <- sender.Send(filesInfo, folders, totalFolders) }()
	go func() { results <- receiver.Receive() }()

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
	return receiveDir, sender, receiver, time.Since(start)
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

// TestSCH05_SingleStreamThrottled512KBpsFastStreamsCarryOver85Percent verifies
// SCH-05's Definition of Done:
//  1. When 1 TCP stream is throttled to 512 KB/s ("0:512k"), fast streams carry
//     >85% of total chunks and receiver counters are never double-counted.
//  2. Dynamic work-stealing eliminates end-of-file waiting on a slow stream
//     compared to static scheduling.
func TestSCH05_SingleStreamThrottled512KBpsFastStreamsCarryOver85Percent(t *testing.T) {
	t.Run("512KBpsFastWorkerShareOver85Percent", func(t *testing.T) {
		srcDir := t.TempDir()
		const chunkSize = models.TCP_BUFFER_SIZE / 2
		const payloadSize = (16 << 20) + 4099 // 513 chunks
		const wantChunks = (payloadSize + chunkSize - 1) / chunkSize

		srcPath, wantSHA := writeDeterministicPayload(t, srcDir, "sch05_512k.bin", payloadSize, 512000)
		metricsPath := filepath.Join(t.TempDir(), "sch05_512k.jsonl")

		recvDir, sender, receiver, _ := runLiveRelayTransferWithClients(
			t,
			4,
			[]string{srcPath},
			Options{
				SchedMode:           SchedModeDynamic,
				DebugThrottleWorker: "0:512k",
				MetricsOut:          metricsPath,
				NoCompress:          true,
			},
			Options{
				SchedMode: SchedModeDynamic,
			},
			nil,
		)

		if gotSHA := sha256File(t, filepath.Join(recvDir, "sch05_512k.bin")); gotSHA != wantSHA {
			t.Fatalf("SHA-256 mismatch on 512 KB/s throttled transfer")
		}

		workerChunks := sender.LastSenderWorkerChunks()
		totalCompleted := 0
		fastCompleted := 0
		for wid, cnt := range workerChunks {
			totalCompleted += cnt
			if wid != 0 {
				fastCompleted += cnt
			}
		}
		if totalCompleted != wantChunks {
			t.Fatalf("total distinct completed chunks = %d, want %d (map=%v)", totalCompleted, wantChunks, workerChunks)
		}
		fastShare := float64(fastCompleted) / float64(totalCompleted)
		if fastShare <= 0.85 {
			t.Fatalf("expected fast streams to carry >85%% of chunks when worker 0 is at 512 KB/s, got %.2f%% (map=%v)",
				fastShare*100, workerChunks)
		}

		receiver.receiveMutex.Lock()
		gotRecvBytes := receiver.TotalSent
		gotRecvChunks := receiver.TotalChunksTransferred
		receiver.receiveMutex.Unlock()
		if gotRecvBytes != payloadSize || gotRecvChunks != wantChunks {
			t.Fatalf("idempotent receiver counters mismatch: bytes=%d (want %d), chunks=%d (want %d)",
				gotRecvBytes, payloadSize, gotRecvChunks, wantChunks)
		}
	})

	t.Run("TailStallEliminatedStaticVsDynamic", func(t *testing.T) {
		srcDir := t.TempDir()
		const payloadSize = (4 << 20) + 1024 // 129 chunks; worker 0 at 32 KiB/s stalls 1s on its 2nd chunk
		srcPath, wantSHA := writeDeterministicPayload(t, srcDir, "tail_ab.bin", payloadSize, 202605)

		recvStatic, _, _, staticDur := runLiveRelayTransferWithClients(
			t,
			4,
			[]string{srcPath},
			Options{
				SchedMode:           SchedModeStatic,
				DebugThrottleWorker: "0:32k",
				NoCompress:          true,
			},
			Options{SchedMode: SchedModeStatic},
			nil,
		)
		if sha256File(t, filepath.Join(recvStatic, "tail_ab.bin")) != wantSHA {
			t.Fatalf("static SHA-256 mismatch")
		}

		metricsDyn := filepath.Join(t.TempDir(), "tail_dyn.jsonl")
		recvDyn, _, _, dynDur := runLiveRelayTransferWithClients(
			t,
			4,
			[]string{srcPath},
			Options{
				SchedMode:           SchedModeDynamic,
				DebugThrottleWorker: "0:32k",
				MetricsOut:          metricsDyn,
				NoCompress:          true,
			},
			Options{SchedMode: SchedModeDynamic},
			nil,
		)
		if sha256File(t, filepath.Join(recvDyn, "tail_ab.bin")) != wantSHA {
			t.Fatalf("dynamic SHA-256 mismatch")
		}

		if dynDur >= staticDur*7/10 {
			t.Fatalf("expected dynamic work-stealing (%v) to eliminate tail stall vs static (%v)", dynDur, staticDur)
		}
		var stolen uint64
		for _, rec := range readStreamStatLines(t, metricsDyn) {
			if rec.StolenChunks > stolen {
				stolen = rec.StolenChunks
			}
		}
		if stolen == 0 {
			t.Fatalf("expected stolen_chunks > 0 during tail-stall elimination")
		}
	})
}

// TestSCH05_IdempotentReceiveNoDoubleCountUnderDuplicateDelivery blasts every
// chunk 3x concurrently across 4 separate data connections into receiveData to
// verify zero double-counting of TotalSent/TotalChunksTransferred and zero
// WriteAt-after-Close (os.ErrClosed) races.
func TestSCH05_IdempotentReceiveNoDoubleCountUnderDuplicateDelivery(t *testing.T) {
	const chunkSize = models.TCP_BUFFER_SIZE / 2
	const numFullChunks = 48
	const tailLen = 3333
	const payloadSize = numFullChunks*chunkSize + tailLen
	const totalDistinctChunks = numFullChunks + 1
	const streams = 4
	const copiesPerChunk = 3

	srcDir := t.TempDir()
	srcPath, wantSHA := writeDeterministicPayload(t, srcDir, "src_dup.bin", payloadSize, 9005)
	payload, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read src: %v", err)
	}

	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "dst_dup.bin")
	outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	if err := outFile.Truncate(payloadSize); err != nil {
		t.Fatalf("truncate dst: %v", err)
	}
	t.Cleanup(func() { _ = outFile.Close() })

	key, _, err := crypt.New([]byte("sch05-dup-key"), []byte("sch05-salt"))
	if err != nil {
		t.Fatalf("crypt.New: %v", err)
	}
	aead, err := crypt.NewAESGCM(key)
	if err != nil {
		t.Fatalf("crypt.NewAESGCM: %v", err)
	}

	newTCPPair := func() (*comm.Comm, *comm.Comm) {
		ln, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			t.Fatalf("net.Listen: %v", listenErr)
		}
		defer ln.Close()
		accepted := make(chan net.Conn, 1)
		go func() {
			sc, _ := ln.Accept()
			accepted <- sc
		}()
		cc, dialErr := net.Dial("tcp", ln.Addr().String())
		if dialErr != nil {
			t.Fatalf("net.Dial: %v", dialErr)
		}
		sc := <-accepted
		t.Cleanup(func() {
			_ = cc.Close()
			if sc != nil {
				_ = sc.Close()
			}
		})
		return comm.New(cc), comm.New(sc)
	}

	ctrlLocal, ctrlRemote := newTCPPair()
	closeDone := make(chan error, 1)
	go func() {
		raw, recvErr := ctrlRemote.Receive()
		if recvErr != nil {
			closeDone <- recvErr
			return
		}
		m, decErr := message.Decode(key, raw)
		if decErr != nil {
			closeDone <- decErr
			return
		}
		if m.Type != message.TypeCloseSender {
			closeDone <- fmt.Errorf("unexpected control message %q", m.Type)
			return
		}
		closeDone <- nil
	}()

	r := &Client{
		Options: Options{
			SharedSecret: "sch05-dup",
			SchedMode:    SchedModeDynamic,
			NoCompress:   true,
			SendingText:  false,
			NoPrompt:     true,
			Overwrite:    true,
			Curve:        "p256",
		},
		Key:                   key,
		dataAEAD:              aead,
		peerDynamicChunkSched: true,
		stop:                  newStop(context.Background()),
		mutex:                 &sync.Mutex{},
		receiveMutex:          &sync.Mutex{},
		FilesToTransfer: []FileInfo{
			{Name: "dst_dup.bin", FolderRemote: outDir, Size: payloadSize},
		},
		FilesToTransferCurrentNum: 0,
		CurrentFile:               outFile,
		CurrentFileChunkCount:     totalDistinctChunks,
		inflightReceiveChunks:     newPagedChunkBitset(chunkSize),
		receivedChunks:            newPagedChunkBitset(chunkSize),
		conn:                      make([]*comm.Comm, streams+1),
	}
	r.conn[0] = ctrlLocal

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	attempt := &transferAttemptState{
		ctx:  ctx,
		errc: make(chan error, 8),
	}

	senderPeers := make([]*comm.Comm, streams)
	var recvWG sync.WaitGroup
	for i := range streams {
		recvComm, sendComm := newTCPPair()
		r.conn[i+1] = recvComm
		senderPeers[i] = sendComm
		recvWG.Add(1)
		go func(idx int, dc *comm.Comm) {
			defer recvWG.Done()
			r.receiveData(idx, dc, attempt)
		}(i, r.conn[i+1])
	}

	perStreamFrames := make([][][]byte, streams)
	for copyIdx := range copiesPerChunk {
		for chIdx := range totalDistinctChunks {
			offset := chIdx * chunkSize
			end := min(offset+chunkSize, payloadSize)
			rawFrame := make([]byte, 8+(end-offset))
			binary.LittleEndian.PutUint64(rawFrame[:8], uint64(offset))
			copy(rawFrame[8:], payload[offset:end])
			encFrame, encErr := crypt.EncryptAEAD(rawFrame, aead)
			if encErr != nil {
				t.Fatalf("encrypt frame: %v", encErr)
			}
			streamIdx := (chIdx + copyIdx) % streams
			perStreamFrames[streamIdx] = append(perStreamFrames[streamIdx], encFrame)
		}
	}

	var sendWG sync.WaitGroup
	for sIdx := range streams {
		sendWG.Add(1)
		go func(dc *comm.Comm, frames [][]byte) {
			defer sendWG.Done()
			for _, frame := range frames {
				if sendErr := dc.Send(frame); sendErr != nil {
					return
				}
			}
		}(senderPeers[sIdx], perStreamFrames[sIdx])
	}
	sendWG.Wait()

	select {
	case ctrlErr := <-closeDone:
		if ctrlErr != nil {
			t.Fatalf("control TypeCloseSender failed: %v", ctrlErr)
		}
	case xferErr := <-attempt.errc:
		t.Fatalf("unexpected receiveData error under duplicate delivery: %v", xferErr)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for TypeCloseSender")
	}

	select {
	case xferErr := <-attempt.errc:
		t.Fatalf("unexpected receiveData error under duplicate delivery: %v", xferErr)
	default:
	}

	cancel()
	for _, sp := range senderPeers {
		sp.Close()
	}
	recvWG.Wait()

	r.receiveMutex.Lock()
	gotBytes := r.TotalSent
	gotChunks := r.TotalChunksTransferred
	r.receiveMutex.Unlock()

	if gotBytes != payloadSize {
		t.Fatalf("TotalSent = %d, want exact %d (no double count)", gotBytes, payloadSize)
	}
	if gotChunks != totalDistinctChunks {
		t.Fatalf("TotalChunksTransferred = %d, want exact %d (no double count)", gotChunks, totalDistinctChunks)
	}
	if sha256File(t, outPath) != wantSHA {
		t.Fatalf("output SHA-256 mismatch after concurrent duplicate writes")
	}
}

// TestSCH05_UnmarkReceivedChunkRangesForSelectiveRepair ensures MRK-04/MRK-05
// repair passes clear only corrupted chunk ranges while preserving duplicate
// protection for already-valid chunks from the previous pass.
func TestSCH05_UnmarkReceivedChunkRangesForSelectiveRepair(t *testing.T) {
	const chunkSize = 32768
	const fileSize = chunkSize*5 + 900 // 6 chunks: 0..5

	c, err := New(Options{
		SharedSecret: "1234-sch05-repair-ranges",
		Curve:        "p256",
		SchedMode:    SchedModeDynamic,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.inflightReceiveChunks = newPagedChunkBitset(chunkSize)
	c.receivedChunks = newPagedChunkBitset(chunkSize)

	for idx := range 6 {
		off := int64(idx * chunkSize)
		c.inflightReceiveChunks.Set(off)
		c.receivedChunks.Set(off)
	}

	// Unmark chunks 1, 2, and tail chunk 5 via ChunkRanges format: [chunkSize, start1, count1, start2, count2].
	cleared := c.unmarkReceivedChunkRanges([]int64{chunkSize, chunkSize, 2, 5 * chunkSize, 1}, fileSize)
	if cleared != 3 {
		t.Fatalf("unmarkReceivedChunkRanges cleared = %d, want 3", cleared)
	}

	wantCleared := map[int]bool{1: true, 2: true, 5: true}
	for idx := range 6 {
		off := int64(idx * chunkSize)
		if wantCleared[idx] {
			if c.receivedChunks.Test(off) || c.inflightReceiveChunks.Test(off) {
				t.Fatalf("expected chunk %d (offset %d) to be unmarked in both bitsets", idx, off)
			}
		} else {
			if !c.receivedChunks.Test(off) || !c.inflightReceiveChunks.Test(off) {
				t.Fatalf("expected healthy chunk %d (offset %d) to remain marked", idx, off)
			}
		}
	}
}

// TestSCH05_MultiFileDrainPreventsCrossFileStaleDuplicates transfers 3 distinct
// files back-to-back with aggressive single-stream throttling to verify zero
// cross-file stale duplicate contamination.
func TestSCH05_MultiFileDrainPreventsCrossFileStaleDuplicates(t *testing.T) {
	srcDir := t.TempDir()
	p1, sha1 := writeDeterministicPayload(t, srcDir, "f1.bin", (3<<20)+111, 701)
	p2, sha2 := writeDeterministicPayload(t, srcDir, "f2.bin", (3<<20)+222, 702)
	p3, sha3 := writeDeterministicPayload(t, srcDir, "f3.bin", (3<<20)+333, 703)

	recvDir := runLiveRelayTransfer(
		t,
		4,
		[]string{p1, p2, p3},
		Options{
			SchedMode:           SchedModeDynamic,
			DebugThrottleWorker: "0:128k",
			NoCompress:          true,
		},
		Options{
			SchedMode: SchedModeDynamic,
		},
		nil,
	)

	if sha256File(t, filepath.Join(recvDir, "f1.bin")) != sha1 {
		t.Fatalf("SHA-256 mismatch on f1.bin")
	}
	if sha256File(t, filepath.Join(recvDir, "f2.bin")) != sha2 {
		t.Fatalf("SHA-256 mismatch on f2.bin")
	}
	if sha256File(t, filepath.Join(recvDir, "f3.bin")) != sha3 {
		t.Fatalf("SHA-256 mismatch on f3.bin")
	}
}

func TestSCH05_PacketDrainAndRangeEdgeCases(t *testing.T) {
	// 1. encodeSentPacketsPayload / decodeSentPacketsPayload boundary cases.
	if b := encodeSentPacketsPayload(5, false); b != nil {
		t.Fatalf("expected nil payload when dynamicActive=false, got %v", b)
	}
	if b := encodeSentPacketsPayload(0, true); b != nil {
		t.Fatalf("expected nil payload when sentPackets=0, got %v", b)
	}
	if _, ok := decodeSentPacketsPayload(nil); ok {
		t.Fatalf("expected empty payload to decode false")
	}
	if _, ok := decodeSentPacketsPayload([]byte{1, 2, 3}); ok {
		t.Fatalf("expected invalid length payload to decode false")
	}
	var overflowBuf [8]byte
	binary.LittleEndian.PutUint64(overflowBuf[:], uint64(math.MaxInt64)+1)
	if _, ok := decodeSentPacketsPayload(overflowBuf[:]); ok {
		t.Fatalf("expected >MaxInt64 payload to decode false")
	}
	validBuf := encodeSentPacketsPayload(42, true)
	if n, ok := decodeSentPacketsPayload(validBuf); !ok || n != 42 {
		t.Fatalf("decodeSentPacketsPayload = (%d, %v), want (42, true)", n, ok)
	}

	// 2. unmarkReceivedChunkRanges guards against non-positive fileSize, mismatching chunkSize, and huge counts.
	c := &Client{
		receiveMutex:          &sync.Mutex{},
		inflightReceiveChunks: newPagedChunkBitset(32768),
		receivedChunks:        newPagedChunkBitset(32768),
	}
	c.receivedChunks.Set(0)
	if got := c.unmarkReceivedChunkRanges([]int64{32768, 0, math.MaxInt64}, 0); got != 0 {
		t.Fatalf("expected 0 when fileSize <= 0, got %d", got)
	}
	if got := c.unmarkReceivedChunkRanges([]int64{16384, 0, 1}, 65536); got != 0 {
		t.Fatalf("expected 0 when chunkSize mismatches bitset chunkSize, got %d", got)
	}
	if got := c.unmarkReceivedChunkRanges([]int64{32768, -1, 1, 100000, 1, 0, math.MaxInt64}, 32768); got != 1 {
		t.Fatalf("expected O(1)-clamped unmark = 1, got %d", got)
	}

	// 3. waitForReceivePacketDrain handles nil c.stop, last-file bypass, nil notifier, timeout error, and cancel.
	bare := &Client{
		Options:               Options{SchedMode: SchedModeDynamic},
		peerDynamicChunkSched: true,
		receiveMutex:          &sync.Mutex{},
		FilesToTransfer:       []FileInfo{{Name: "a"}, {Name: "b"}},
	}
	// Missing recvDrainNotify triggers explicit error rather than hanging or panicking with nil c.stop.
	if err := bare.waitForReceivePacketDrainWithTimeout(nil, 2, 10*time.Millisecond); err == nil {
		t.Fatalf("expected error when recvDrainNotify is nil and packets < target")
	}
	// Timeout returns transferDisconnectError rather than silently proceeding.
	bare.recvDrainNotify = make(chan struct{}, 1)
	if err := bare.waitForReceivePacketDrainWithTimeout(nil, 2, 10*time.Millisecond); err == nil {
		t.Fatalf("expected timeout error when delayed packets fail to arrive")
	} else {
		var disc transferDisconnectError
		if !errors.As(err, &disc) {
			t.Fatalf("expected transferDisconnectError on drain timeout, got %T (%v)", err, err)
		}
	}
	// Last file bypasses drain even with unreceived duplicates.
	bare.FilesToTransferCurrentNum = 1
	if err := bare.waitForReceivePacketDrainWithTimeout(nil, 10, 10*time.Millisecond); err != nil {
		t.Fatalf("expected last-file drain bypass, got %v", err)
	}
	// Context cancellation stops drain wait cleanly.
	bare.FilesToTransferCurrentNum = 0
	ctx, cancel := context.WithCancel(context.Background())
	attempt := &transferAttemptState{ctx: ctx}
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	if err := bare.waitForReceivePacketDrainWithTimeout(attempt, 5, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from drain wait, got %v", err)
	}
}

// TestSCH05_MixedCompressionAndAsymmetricMultiFile verifies:
//  1. Multi-file transfers mixing compressible text payloads and incompressible
//     random binary payloads with NoCompress=false and a throttled stream.
//  2. Asymmetric negotiation (--sched static sender vs --sched dynamic receiver)
//     safely completes multi-file transfers without deadlocking or corrupting files.
func TestSCH05_MixedCompressionAndAsymmetricMultiFile(t *testing.T) {
	t.Run("MixedCompressionWithThrottle", func(t *testing.T) {
		srcDir := t.TempDir()
		textPath := filepath.Join(srcDir, "compressible.txt")
		textBytes := bytes.Repeat([]byte("croc-sch05-compressible-telemetry-record\n"), 20000)
		if err := os.WriteFile(textPath, textBytes, 0o600); err != nil {
			t.Fatalf("write compressible: %v", err)
		}
		textSHA := sha256.Sum256(textBytes)

		binPath, binSHA := writeDeterministicPayload(t, srcDir, "incompressible.bin", (2<<20)+123, 888)

		recvDir := runLiveRelayTransfer(
			t,
			4,
			[]string{textPath, binPath},
			Options{
				SchedMode:           SchedModeDynamic,
				DebugThrottleWorker: "0:256k",
				NoCompress:          false,
			},
			Options{
				SchedMode:  SchedModeDynamic,
				NoCompress: false,
			},
			nil,
		)

		if sha256File(t, filepath.Join(recvDir, "compressible.txt")) != textSHA {
			t.Fatalf("SHA-256 mismatch on compressible.txt")
		}
		if sha256File(t, filepath.Join(recvDir, "incompressible.bin")) != binSHA {
			t.Fatalf("SHA-256 mismatch on incompressible.bin")
		}
	})

	t.Run("AsymmetricStaticSenderDynamicReceiver", func(t *testing.T) {
		srcDir := t.TempDir()
		p1, sha1 := writeDeterministicPayload(t, srcDir, "asym1.bin", (1<<20)+77, 11)
		p2, sha2 := writeDeterministicPayload(t, srcDir, "asym2.bin", (1<<20)+99, 22)

		recvDir := runLiveRelayTransfer(
			t,
			4,
			[]string{p1, p2},
			Options{
				SchedMode:  SchedModeStatic,
				NoCompress: true,
			},
			Options{
				SchedMode:  SchedModeDynamic,
				NoCompress: true,
			},
			nil,
		)

		if sha256File(t, filepath.Join(recvDir, "asym1.bin")) != sha1 {
			t.Fatalf("SHA-256 mismatch on asym1.bin")
		}
		if sha256File(t, filepath.Join(recvDir, "asym2.bin")) != sha2 {
			t.Fatalf("SHA-256 mismatch on asym2.bin")
		}
	})
}
