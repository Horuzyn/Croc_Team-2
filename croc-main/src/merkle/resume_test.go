package merkle

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/schollz/croc/v11/src/utils"
)

func TestVerifiedMissingChunks_50PercentPartialPlus3CorruptedChunks(t *testing.T) {
	// MRK-03 DoD: Create a 50% partial file and overwrite garbage into 3 arbitrary
	// received chunks -> accurately detect the 3 corrupted chunks + 50% missing tail.
	const (
		chunkSize   = DefaultChunkSize // 32 KiB
		totalChunks = 100              // ~3.2 MiB
		halfChunks  = 50               // 50% received, 50% missing
	)
	// Make final chunk unaligned (-321 bytes) to exercise partial tail accounting too.
	fileSize := int64(totalChunks*chunkSize - 321)

	origin := make([]byte, fileSize)
	if _, err := rand.Read(origin); err != nil {
		t.Fatal(err)
	}

	// Ensure chunk 10 in the original file is legitimately all-zero (0x00) to prove
	// VerifiedMissingChunks improves on legacy utils.MissingChunksFile by NOT
	// misclassifying valid all-zero sender chunks as missing!
	copy(origin[10*chunkSize:11*chunkSize], make([]byte, chunkSize))

	senderTree, err := BuildTreeFromBytes(origin, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	remoteLeaves := senderTree.LeafHashes()

	corruptIndices := []int{3, 19, 44}

	t.Run("preallocated_sparse_tail", func(t *testing.T) {
		// Simulates croc's recipientInitializeFile: truncated to full fileSize with
		// second 50% still all-zero on disk, and 3 chunks in first 50% corrupted.
		partialPath := filepath.Join(t.TempDir(), "partial_sparse.bin")
		f, err := os.OpenFile(partialPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(fileSize); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if _, err := f.WriteAt(origin[:halfChunks*chunkSize], 0); err != nil {
			f.Close()
			t.Fatal(err)
		}
		garbage := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, chunkSize/4)
		for _, idx := range corruptIndices {
			if _, err := f.WriteAt(garbage, int64(idx*chunkSize)); err != nil {
				f.Close()
				t.Fatal(err)
			}
		}

		// Record seek position to confirm VerifiedMissingChunksFile does not move it.
		if _, err := f.Seek(12345, 0); err != nil {
			f.Close()
			t.Fatal(err)
		}

		res, err := ScanResumeFile(f, fileSize, chunkSize, remoteLeaves)
		if err != nil {
			f.Close()
			t.Fatalf("ScanResumeFile error: %v", err)
		}
		pos, err := f.Seek(0, 1)
		f.Close()
		if err != nil || pos != 12345 {
			t.Fatalf("file seek offset changed to %d (err=%v), want 12345", pos, err)
		}

		step := int64(chunkSize)
		wantRanges := []int64{
			step,
			3 * step, 1,
			19 * step, 1,
			44 * step, 1,
			50 * step, 50,
		}
		if !slices.Equal(res.Ranges, wantRanges) {
			t.Fatalf("Ranges = %v, want %v", res.Ranges, wantRanges)
		}
		if res.CorruptChunks != 3 {
			t.Fatalf("CorruptChunks = %d, want 3", res.CorruptChunks)
		}
		if res.MissingChunks != 50 {
			t.Fatalf("MissingChunks = %d, want 50", res.MissingChunks)
		}
		if res.ValidChunks != 47 {
			t.Fatalf("ValidChunks = %d, want 47 (including valid all-zero chunk #10)", res.ValidChunks)
		}

		wantRetransmit := utils.ChunkRangesBytes(wantRanges, fileSize, step)
		if res.RetransmittedBytes != wantRetransmit {
			t.Fatalf("RetransmittedBytes = %d, want %d", res.RetransmittedBytes, wantRetransmit)
		}
		if res.SavedBytes != fileSize-wantRetransmit {
			t.Fatalf("SavedBytes = %d, want %d", res.SavedBytes, fileSize-wantRetransmit)
		}
	})

	t.Run("unallocated_short_file_50pct", func(t *testing.T) {
		// Simulates a partial file on disk whose size is physically 50% of fileSize.
		shortPath := filepath.Join(t.TempDir(), "partial_short.bin")
		partialBytes := bytes.Clone(origin[:halfChunks*chunkSize])
		for _, idx := range corruptIndices {
			partialBytes[idx*chunkSize+7] ^= 0x01 // single-bit corruption
		}
		if err := os.WriteFile(shortPath, partialBytes, 0o600); err != nil {
			t.Fatal(err)
		}

		ranges, err := VerifiedMissingChunks(shortPath, fileSize, chunkSize, remoteLeaves)
		if err != nil {
			t.Fatalf("VerifiedMissingChunks error: %v", err)
		}

		step := int64(chunkSize)
		wantRanges := []int64{
			step,
			3 * step, 1,
			19 * step, 1,
			44 * step, 1,
			50 * step, 50,
		}
		if !slices.Equal(ranges, wantRanges) {
			t.Fatalf("VerifiedMissingChunks = %v, want %v", ranges, wantRanges)
		}
	})
}

func TestVerifiedMissingChunks_CompleteMissingAndEmptyCases(t *testing.T) {
	const chunkSize = 8192
	data := make([]byte, 5*chunkSize+199)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	tree, err := BuildTreeFromBytes(data, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	leaves := tree.LeafHashes()
	dir := t.TempDir()

	// 1. Intact file -> nil ranges.
	okPath := filepath.Join(dir, "intact.bin")
	if err := os.WriteFile(okPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ranges, err := VerifiedMissingChunks(okPath, int64(len(data)), chunkSize, leaves)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 0 {
		t.Fatalf("intact file returned non-empty ranges: %v", ranges)
	}

	// 2. Non-existent file -> full range [chunkSize, 0, 6].
	missingPath := filepath.Join(dir, "does_not_exist.bin")
	ranges, err = VerifiedMissingChunks(missingPath, int64(len(data)), chunkSize, leaves)
	if err != nil {
		t.Fatal(err)
	}
	wantFull := []int64{chunkSize, 0, 6}
	if !slices.Equal(ranges, wantFull) {
		t.Fatalf("non-existent file ranges = %v, want %v", ranges, wantFull)
	}

	// 3. 0-byte file -> nil ranges.
	ranges, err = VerifiedMissingChunks(missingPath, 0, chunkSize, nil)
	if err != nil || len(ranges) != 0 {
		t.Fatalf("0-byte file got (%v, %v), want (nil, nil)", ranges, err)
	}

	// 4. Mismatched leaf count -> ErrLeafCountMismatch.
	if _, err := VerifiedMissingChunks(okPath, int64(len(data)), chunkSize, leaves[:2]); !errors.Is(err, ErrLeafCountMismatch) {
		t.Fatalf("got err %v, want ErrLeafCountMismatch", err)
	}

	// 5. VerifiedMissingChunksFile wrapper + 0-byte empty file on disk + nil *os.File check.
	okFile, err := os.Open(okPath)
	if err != nil {
		t.Fatal(err)
	}
	defer okFile.Close()
	if got, err := VerifiedMissingChunksFile(okFile, int64(len(data)), chunkSize, leaves); err != nil || len(got) != 0 {
		t.Fatalf("VerifiedMissingChunksFile(intact) = (%v, %v), want (nil, nil)", got, err)
	}

	emptyDiskPath := filepath.Join(dir, "empty_disk.bin")
	if err := os.WriteFile(emptyDiskPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	emptyDiskFile, err := os.Open(emptyDiskPath)
	if err != nil {
		t.Fatal(err)
	}
	defer emptyDiskFile.Close()
	if got, err := VerifiedMissingChunksFile(emptyDiskFile, int64(len(data)), chunkSize, leaves); err != nil || !slices.Equal(got, wantFull) {
		t.Fatalf("VerifiedMissingChunksFile(0B disk for non-empty remote) = (%v, %v), want (%v, nil)", got, err, wantFull)
	}
	if got, err := VerifiedMissingChunksFile(emptyDiskFile, 0, chunkSize, nil); err != nil || len(got) != 0 {
		t.Fatalf("VerifiedMissingChunksFile(0B file) = (%v, %v), want (nil, nil)", got, err)
	}
	if _, err := VerifiedMissingChunksFile(nil, int64(len(data)), chunkSize, leaves); err == nil {
		t.Fatal("VerifiedMissingChunksFile(nil) succeeded")
	}
}

func TestVerifiedMissingChunks_ConcurrentCallsRaceSafe(t *testing.T) {
	const chunkSize = 4096
	data := make([]byte, 32*chunkSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	tree, err := BuildTreeFromBytes(data, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	leaves := tree.LeafHashes()

	fpath := filepath.Join(t.TempDir(), "concurrent.bin")
	corrupted := bytes.Clone(data)
	corrupted[5*chunkSize] ^= 0xFF
	corrupted[21*chunkSize+42] ^= 0x80
	if err := os.WriteFile(fpath, corrupted, 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(fpath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	want := []int64{chunkSize, 5 * chunkSize, 1, 21 * chunkSize, 1}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(workers int) {
			defer wg.Done()
			res, err := ScanResumeFileWorkers(f, int64(len(data)), chunkSize, leaves, workers)
			if err != nil {
				t.Errorf("ScanResumeFileWorkers error: %v", err)
				return
			}
			if !slices.Equal(res.Ranges, want) {
				t.Errorf("Ranges = %v, want %v", res.Ranges, want)
			}
		}(g%4 + 1)
	}
	wg.Wait()
}

func BenchmarkMerkleVerifiedMissingChunks8MiB(b *testing.B) {
	const size = 8 * 1024 * 1024
	data := make([]byte, size)
	_, _ = rand.Read(data)
	tree, err := BuildTreeFromBytes(data, DefaultChunkSize)
	if err != nil {
		b.Fatal(err)
	}
	leaves := tree.LeafHashes()

	fpath := filepath.Join(b.TempDir(), "resume8m.bin")
	// 50% written, 2 chunks corrupted.
	partial := make([]byte, size)
	copy(partial, data[:size/2])
	partial[3*DefaultChunkSize] ^= 0x01
	partial[42*DefaultChunkSize] ^= 0x01
	if err := os.WriteFile(fpath, partial, 0o600); err != nil {
		b.Fatal(err)
	}

	f, err := os.Open(fpath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := VerifiedMissingChunksFile(f, size, DefaultChunkSize, leaves); err != nil {
			b.Fatal(err)
		}
	}
}
