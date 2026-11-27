package merkle

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/schollz/croc/v11/src/utils"
)

func TestDomainSeparationAndEmptyTree(t *testing.T) {
	empty := EmptyRoot()
	wantEmpty := sha256.Sum256([]byte{0x00})
	if empty != wantEmpty {
		t.Fatalf("EmptyRoot() = %x, want %x", empty, wantEmpty)
	}

	// Single zero byte leaf must NOT collide with 0-byte EmptyRoot.
	zeroByteLeaf := LeafHash([]byte{0x00})
	if zeroByteLeaf == empty {
		t.Fatal("1-byte 0x00 leaf collided with EmptyRoot")
	}

	// Internal node must NOT collide with a 64-byte leaf containing left||right.
	left := LeafHash([]byte("left-chunk"))
	right := LeafHash([]byte("right-chunk"))
	var concat [64]byte
	copy(concat[:32], left[:])
	copy(concat[32:], right[:])
	if NodeHash(left, right) == LeafHash(concat[:]) {
		t.Fatal("NodeHash collided with LeafHash(left||right); domain separation broken")
	}
}

func TestBuildTreeEdgeCases(t *testing.T) {
	dir := t.TempDir()
	const chunkSize = DefaultChunkSize // 32 KiB

	cases := []struct {
		name       string
		size       int
		wantLeaves int
	}{
		{name: "empty_0B", size: 0, wantLeaves: 0},
		{name: "tiny_1B", size: 1, wantLeaves: 1},
		{name: "sub_chunk_17KiB", size: 17 * 1024, wantLeaves: 1},
		{name: "exact_1_chunk_32KiB", size: chunkSize, wantLeaves: 1},
		{name: "two_chunks_exact_64KiB", size: 2 * chunkSize, wantLeaves: 2},
		{name: "odd_3_chunks_unaligned", size: 2*chunkSize + 513, wantLeaves: 3},
		{name: "odd_5_chunks_unaligned", size: 4*chunkSize + 32767, wantLeaves: 5},
		{name: "many_chunks_19_leaves", size: 18*chunkSize + 7, wantLeaves: 19},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			if tc.size > 0 {
				if _, err := rand.Read(data); err != nil {
					t.Fatal(err)
				}
			}
			fpath := filepath.Join(dir, tc.name+".bin")
			if err := os.WriteFile(fpath, data, 0o600); err != nil {
				t.Fatal(err)
			}

			fileTree, err := BuildTreeFromFile(fpath, chunkSize)
			if err != nil {
				t.Fatalf("BuildTreeFromFile error: %v", err)
			}
			memTree, err := BuildTreeFromBytes(data, chunkSize)
			if err != nil {
				t.Fatalf("BuildTreeFromBytes error: %v", err)
			}

			if fileTree.LeafCount() != tc.wantLeaves {
				t.Fatalf("LeafCount() = %d, want %d", fileTree.LeafCount(), tc.wantLeaves)
			}
			if fileTree.FileSize() != int64(tc.size) {
				t.Fatalf("FileSize() = %d, want %d", fileTree.FileSize(), tc.size)
			}
			if fileTree.Root() != memTree.Root() {
				t.Fatalf("file root %x != memory root %x", fileTree.Root(), memTree.Root())
			}
			if !slices.Equal(fileTree.LeafHashes(), memTree.LeafHashes()) {
				t.Fatal("LeafHashes mismatch between file and memory builders")
			}

			// Reconstruct from packed leaves (simulating MerkleManifest wire transfer).
			packed := PackHashes(fileTree.LeafHashes())
			unpacked, err := UnpackHashes(packed)
			if err != nil {
				t.Fatalf("UnpackHashes error: %v", err)
			}
			rebuilt, err := NewTreeFromLeaves(unpacked, int64(tc.size), chunkSize)
			if err != nil {
				t.Fatalf("NewTreeFromLeaves error: %v", err)
			}
			if rebuilt.Root() != fileTree.Root() {
				t.Fatalf("rebuilt root %x != original root %x", rebuilt.Root(), fileTree.Root())
			}
		})
	}
}

func TestSingleBitFlipDetectedAtEveryChunk(t *testing.T) {
	const chunkSize = 4096
	// 9 chunks: 8 full chunks + 1 partial chunk of 333 bytes.
	totalSize := 8*chunkSize + 333
	data := make([]byte, totalSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	fpath := filepath.Join(dir, "bitflip.bin")
	if err := os.WriteFile(fpath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	tree, err := BuildTreeFromFile(fpath, chunkSize)
	if err != nil {
		t.Fatal(err)
	}

	for idx := range tree.LeafCount() {
		start := idx * chunkSize
		end := min(start+chunkSize, len(data))
		origChunk := data[start:end]

		if !tree.VerifyChunk(idx, origChunk) {
			t.Fatalf("valid chunk %d failed VerifyChunk", idx)
		}

		// Flip 1 bit at first byte, middle byte, and last byte of chunk idx.
		for _, pos := range []int{0, len(origChunk) / 2, len(origChunk) - 1} {
			for bit := range 8 {
				corrupted := bytes.Clone(origChunk)
				corrupted[pos] ^= byte(1 << bit)
				if tree.VerifyChunk(idx, corrupted) {
					t.Fatalf("chunk %d byte %d bit %d flip was NOT detected", idx, pos, bit)
				}
			}
		}

		// Truncated or extended chunk data must also fail verification.
		if len(origChunk) > 1 && tree.VerifyChunk(idx, origChunk[:len(origChunk)-1]) {
			t.Fatalf("truncated chunk %d falsely passed VerifyChunk", idx)
		}
		extended := append(bytes.Clone(origChunk), 0xAB)
		if tree.VerifyChunk(idx, extended) {
			t.Fatalf("extended chunk %d falsely passed VerifyChunk", idx)
		}
	}

	if tree.VerifyChunk(-1, data[:chunkSize]) || tree.VerifyChunk(tree.LeafCount(), data[:chunkSize]) {
		t.Fatal("out-of-bounds index passed VerifyChunk")
	}
}

func TestDiffLeavesMatchesCrocChunkRanges(t *testing.T) {
	const chunkSize = DefaultChunkSize
	const numLeaves = 10
	fileSize := int64(numLeaves*chunkSize - 500)

	remote := make([]Hash, numLeaves)
	for i := range remote {
		remote[i] = LeafHash([]byte{byte(i), 0xAA, 0x55})
	}

	// Identical leaves -> nil ranges.
	if got := DiffLeaves(remote, remote); got != nil {
		t.Fatalf("DiffLeaves(identical) = %v, want nil", got)
	}

	// Corrupt indices 0, 4, 5, and leave indices 7, 8, 9 missing (shorter local slice).
	local := slices.Clone(remote[:7])
	local[0] = LeafHash([]byte("corrupt-0"))
	local[4] = LeafHash([]byte("corrupt-4"))
	local[5] = LeafHash([]byte("corrupt-5"))

	gotRanges := DiffLeaves(local, remote)
	step := int64(chunkSize)
	wantRanges := []int64{
		step,
		0 * step, 1,
		4 * step, 2,
		7 * step, 3,
	}
	if !slices.Equal(gotRanges, wantRanges) {
		t.Fatalf("DiffLeaves = %v, want %v", gotRanges, wantRanges)
	}

	// Verify 100% compatibility with croc's utils.ChunkRanges* helpers.
	gotOffsets := utils.ChunkRangesToChunks(gotRanges)
	wantOffsets := []int64{0 * step, 4 * step, 5 * step, 7 * step, 8 * step, 9 * step}
	if !slices.Equal(gotOffsets, wantOffsets) {
		t.Fatalf("ChunkRangesToChunks = %v, want %v", gotOffsets, wantOffsets)
	}
	if cnt := utils.ChunkRangesCount(gotRanges, fileSize, step); cnt != 6 {
		t.Fatalf("ChunkRangesCount = %d, want 6", cnt)
	}
	wantBytes := 5*step + (fileSize - 9*step)
	if b := utils.ChunkRangesBytes(gotRanges, fileSize, step); b != wantBytes {
		t.Fatalf("ChunkRangesBytes = %d, want %d", b, wantBytes)
	}
}

func TestDefensiveImmutabilityAndInvalidArgs(t *testing.T) {
	if _, err := BuildTreeFromBytes([]byte("abc"), 0); !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("got err %v, want ErrInvalidChunkSize", err)
	}
	if _, err := BuildTreeFromFile("nonexistent", -5); !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("got err %v, want ErrInvalidChunkSize", err)
	}
	if _, err := UnpackHashes(make([]byte, 33)); !errors.Is(err, ErrInvalidPackedHashes) {
		t.Fatalf("got err %v, want ErrInvalidPackedHashes", err)
	}
	if _, err := NewTreeFromLeaves(make([]Hash, 2), int64(DefaultChunkSize), DefaultChunkSize); !errors.Is(err, ErrLeafCountMismatch) {
		t.Fatalf("got err %v, want ErrLeafCountMismatch", err)
	}

	tree, err := BuildTreeFromBytes([]byte("hello world"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if tree.ChunkSize() != 4 {
		t.Fatalf("ChunkSize() = %d, want 4", tree.ChunkSize())
	}
	if h0, ok := tree.LeafHashAt(0); !ok || h0 != LeafHash([]byte("hell")) {
		t.Fatalf("LeafHashAt(0) = (%x, %v)", h0, ok)
	}
	if _, ok := tree.LeafHashAt(-1); ok {
		t.Fatal("LeafHashAt(-1) returned true")
	}
	if _, ok := tree.LeafHashAt(99); ok {
		t.Fatal("LeafHashAt(99) returned true")
	}

	leaves := tree.LeafHashes()
	leaves[0] = Hash{}
	if !tree.VerifyChunk(0, []byte("hell")) {
		t.Fatal("mutating returned LeafHashes() corrupted internal tree state")
	}

	// Nil receiver safety and empty helper edge cases.
	var nilTree *Tree
	if nilTree.Root() != EmptyRoot() || nilTree.LeafHashes() != nil || nilTree.LeafCount() != 0 ||
		nilTree.ChunkSize() != 0 || nilTree.FileSize() != 0 || nilTree.VerifyChunk(0, []byte("a")) {
		t.Fatal("nil *Tree methods did not return safe zero values")
	}
	if _, ok := nilTree.LeafHashAt(0); ok {
		t.Fatal("nilTree.LeafHashAt returned true")
	}
	if PackHashes(nil) != nil {
		t.Fatal("PackHashes(nil) != nil")
	}
	if got, err := UnpackHashes(nil); err != nil || got != nil {
		t.Fatalf("UnpackHashes(nil) = (%v, %v), want (nil, nil)", got, err)
	}
	if DiffLeaves(nil, nil) != nil || DiffLeavesChunkSize(leaves, leaves, 0) != nil {
		t.Fatal("DiffLeaves empty/default chunkSize cases failed")
	}
	if _, err := ExpectedLeafCount(-1, DefaultChunkSize); !errors.Is(err, ErrInvalidFileSize) {
		t.Fatalf("ExpectedLeafCount(-1) = %v, want ErrInvalidFileSize", err)
	}
	if _, err := BuildTreeFromOpenedFile(nil, DefaultChunkSize); err == nil {
		t.Fatal("BuildTreeFromOpenedFile(nil) succeeded")
	}
}

func BenchmarkMerkleBuildTree8MiB(b *testing.B) {
	const size = 8 * 1024 * 1024
	data := make([]byte, size)
	_, _ = rand.Read(data)
	fpath := filepath.Join(b.TempDir(), "bench8m.bin")
	if err := os.WriteFile(fpath, data, 0o600); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := BuildTreeFromFile(fpath, DefaultChunkSize); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMerkleVerifyChunk32KiB(b *testing.B) {
	chunk := make([]byte, DefaultChunkSize)
	_, _ = rand.Read(chunk)
	tree, err := BuildTreeFromBytes(chunk, DefaultChunkSize)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(DefaultChunkSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !tree.VerifyChunk(0, chunk) {
			b.Fatal("VerifyChunk failed")
		}
	}
}
