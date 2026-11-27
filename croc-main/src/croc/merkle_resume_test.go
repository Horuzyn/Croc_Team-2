package croc

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/merkle"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/telemetry"
)

func TestMerkleFeatureNegotiationAndLegacyFallback(t *testing.T) {
	cDisabled := &Client{Options: Options{MerkleResume: false}}
	if slices.Contains(cDisabled.pakeFeatures(), message.FeatureMerkleChunkResumeV1) {
		t.Fatal("pakeFeatures() advertised merkle-chunk-resume-v1 when MerkleResume=false")
	}
	if cDisabled.merkleResumeActive() {
		t.Fatal("merkleResumeActive() should be false when MerkleResume=false")
	}

	cEnabled := &Client{Options: Options{MerkleResume: true}}
	if !slices.Contains(cEnabled.pakeFeatures(), message.FeatureMerkleChunkResumeV1) {
		t.Fatal("pakeFeatures() did not advertise merkle-chunk-resume-v1 when MerkleResume=true")
	}
	if cEnabled.merkleResumeActive() {
		t.Fatal("merkleResumeActive() should be false before peer advertises feature")
	}

	cEnabled.peerMerkleChunkResume = true
	if !cEnabled.merkleResumeActive() {
		t.Fatal("merkleResumeActive() should be true when both peers support it")
	}
}

func TestMerkleManifestValidationAndInlineRepairLimit(t *testing.T) {
	const chunkSize = models.TCP_BUFFER_SIZE / 2
	data := make([]byte, 4*chunkSize+777)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	tree, err := merkle.BuildTreeFromBytes(data, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	root := tree.Root()
	leaves := tree.LeafHashes()

	metricsPath := filepath.Join(t.TempDir(), "metrics.jsonl")
	sink, err := telemetry.Open(metricsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	recv := &Client{
		Options:                   Options{IsSender: false, MerkleResume: true},
		Key:                       []byte("0123456789abcdef0123456789abcdef"),
		Step1ChannelSecured:       true,
		peerMerkleChunkResume:     true,
		FilesToTransferCurrentNum: 0,
		metrics:                   sink,
	}

	// Split 5 leaves across two manifest batches (2 + 3) to exercise batching.
	for _, b := range []struct {
		start int
		end   int
		final bool
	}{
		{0, 2, false},
		{2, len(leaves), true},
	} {
		payload, err := json.Marshal(merkleManifestPayload{
			FileIndex:  0,
			FileSize:   int64(len(data)),
			ChunkSize:  chunkSize,
			LeafCount:  len(leaves),
			Root:       root[:],
			StartLeaf:  b.start,
			LeafHashes: merkle.PackHashes(leaves[b.start:b.end]),
			Final:      b.final,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := recv.processMessageMerkleManifest(message.Message{
			Type:  message.TypeMerkleManifest,
			Bytes: payload,
		}); err != nil {
			t.Fatalf("processMessageMerkleManifest(%d..%d) error: %v", b.start, b.end, err)
		}
	}

	if recv.merkleTreeForFile(0) == nil || recv.merkleTreeForFile(0).Root() != root {
		t.Fatal("receiver did not reconstruct expected Merkle tree")
	}

	// Simulate receiving 4 valid chunks and 1 corrupted chunk (index 2).
	for idx := range 5 {
		start := idx * chunkSize
		end := min(start+chunkSize, len(data))
		chunk := bytes.Clone(data[start:end])
		if idx == 2 {
			chunk[100] ^= 0x40
		}
		recv.verifyIncomingChunk(int64(start), chunk)
	}

	for round := 1; round <= maxMerkleRepairRounds; round++ {
		ranges, gotRound, err := recv.finishOrRequestMerkleRepair(int64(len(data)))
		if err != nil {
			t.Fatalf("round %d unexpected error: %v", round, err)
		}
		if gotRound != round {
			t.Fatalf("gotRound = %d, want %d", gotRound, round)
		}
		wantRanges := []int64{int64(chunkSize), int64(2 * chunkSize), 1}
		if !slices.Equal(ranges, wantRanges) {
			t.Fatalf("repair ranges = %v, want %v", ranges, wantRanges)
		}
	}

	// Round 4 must fail cleanly rather than looping infinitely.
	if _, _, err := recv.finishOrRequestMerkleRepair(int64(len(data))); err == nil {
		t.Fatal("expected error after exceeding maxMerkleRepairRounds")
	}

	// Re-delivering valid chunk 2 clears the corruption state.
	recv.resetMerkleFileState()
	recv.verifyIncomingChunk(int64(2*chunkSize), data[2*chunkSize:3*chunkSize])
	ranges, _, err := recv.finishOrRequestMerkleRepair(int64(len(data)))
	if err != nil || len(ranges) != 0 {
		t.Fatalf("after valid repair got (%v, %v), want (nil, nil)", ranges, err)
	}
}

func TestMerkleResumeE2E_10MBWith2CorruptChunks(t *testing.T) {
	// MRK-05 DoD: 10 MB transfer, interrupted at 50% with 2 corrupted chunks (64 KiB)
	// on disk; resume transfers only missing 50% + 64 KiB corrupted, final SHA-256 matches 100%.
	const (
		chunkSize = models.TCP_BUFFER_SIZE / 2 // 32 KiB
		fileSize  = 10 * 1024 * 1024           // 10 MiB (320 chunks)
		halfBytes = fileSize / 2
	)
	relay := startTestRelay(t, 4)

	testRoot := t.TempDir()
	sourceDir := filepath.Join(testRoot, "source")
	receiveDir := filepath.Join(testRoot, "receive")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(receiveDir, 0o700); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, fileSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	wantSum := sha256.Sum256(payload)

	const fileName = "payload_10mb.bin"
	if err := os.WriteFile(filepath.Join(sourceDir, fileName), payload, 0o600); err != nil {
		t.Fatal(err)
	}

	// Pre-populate receiver with 50% of the file and corrupt 2 chunks (2 * 32 KiB = 64 KiB).
	partial := make([]byte, fileSize)
	copy(partial[:halfBytes], payload[:halfBytes])
	partial[7*chunkSize+13] ^= 0xFF
	partial[88*chunkSize+500] ^= 0x3C
	if err := os.WriteFile(filepath.Join(receiveDir, fileName), partial, 0o600); err != nil {
		t.Fatal(err)
	}

	metricsPath := filepath.Join(testRoot, "merkle_metrics.jsonl")
	secret := fmt.Sprintf("merkle-e2e-%d", time.Now().UnixNano())

	sender, err := New(Options{
		IsSender:       true,
		SharedSecret:   secret,
		Curve:          "p256",
		RelayAddress:   relay.address,
		RelayPassword:  models.DEFAULT_PASSPHRASE,
		RelayPorts:     relay.dataPorts,
		DisableLocal:   true,
		NoMultiplexing: true,
		NoCompress:     true,
		MerkleResume:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := New(Options{
		IsSender:       false,
		SharedSecret:   secret,
		Curve:          "p256",
		RelayAddress:   relay.address,
		RelayPassword:  models.DEFAULT_PASSPHRASE,
		RelayPorts:     relay.dataPorts,
		DisableLocal:   true,
		NoMultiplexing: true,
		NoPrompt:       true,
		Overwrite:      true,
		OutDir:         receiveDir,
		MerkleResume:   true,
		MetricsOut:     metricsPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	filesInfo, emptyFolders, totalFolders, err := GetFilesInfo(
		[]string{filepath.Join(sourceDir, fileName)},
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	recvErrCh := make(chan error, 1)
	go func() {
		recvErrCh <- receiver.Receive()
	}()

	if err := sender.Send(filesInfo, emptyFolders, totalFolders); err != nil {
		t.Fatalf("sender.Send error: %v", err)
	}
	if err := <-recvErrCh; err != nil {
		t.Fatalf("receiver.Receive error: %v", err)
	}

	gotBytes, err := os.ReadFile(filepath.Join(receiveDir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	gotSum := sha256.Sum256(gotBytes)
	if gotSum != wantSum {
		t.Fatalf("final SHA-256 mismatch: got %x, want %x", gotSum, wantSum)
	}

	metricsRaw, err := os.ReadFile(metricsPath)
	if err != nil {
		t.Fatal(err)
	}
	var foundResumeVerify bool
	for line := range strings.SplitSeq(strings.TrimSpace(string(metricsRaw)), "\n") {
		if line == "" {
			continue
		}
		var ev struct {
			Event              string `json:"event"`
			CorruptChunks      int    `json:"corrupt_chunks"`
			RetransmittedBytes int64  `json:"retransmitted_bytes"`
			SavedBytes         int64  `json:"saved_bytes"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("decode telemetry line %q: %v", line, err)
		}
		if ev.Event == "merkle_verify" && ev.CorruptChunks == 2 {
			foundResumeVerify = true
			wantRetransmit := int64(halfBytes + 2*chunkSize)
			wantSaved := int64(halfBytes - 2*chunkSize)
			if ev.RetransmittedBytes != wantRetransmit {
				t.Fatalf("RetransmittedBytes = %d, want %d", ev.RetransmittedBytes, wantRetransmit)
			}
			if ev.SavedBytes != wantSaved {
				t.Fatalf("SavedBytes = %d, want %d", ev.SavedBytes, wantSaved)
			}
		}
	}
	if !foundResumeVerify {
		t.Fatalf("did not find merkle_verify telemetry event with corrupt_chunks=2 in:\n%s", string(metricsRaw))
	}
}
