// Package merkle implements a chunk-level SHA-256 Merkle tree used for
// verified transfer resume and selective chunk repair in croc.
//
// Tree Specification (MRK-01 / MRK-02):
//   - Default leaf chunk size: 32 KiB (DefaultChunkSize = 32 * 1024), matching
//     croc's wire data chunk size (models.TCP_BUFFER_SIZE / 2).
//   - Domain separation (RFC 6962 §2.1):
//     Leaf hash:     SHA256(0x00 || chunkBytes)
//     Internal node: SHA256(0x01 || leftHash || rightHash)
//   - Edge cases:
//     1. 0-byte file (0 leaves): LeafCount() == 0, Root() == EmptyRoot()
//     which is SHA256(0x00). Because every leaf of a non-empty file hashes at
//     least 1 payload byte after 0x00, EmptyRoot() cannot collide with any
//     non-empty file leaf or any internal node (0x01 prefix).
//     2. File < chunkSize (1 leaf): LeafCount() == 1, Root() == leaves[0].
//     3. File not divisible by chunkSize: the final leaf hashes the exact
//     remaining (fileSize % chunkSize) bytes without zero-padding.
//     4. Odd node count at a tree level: the trailing unpaired node is promoted
//     unchanged to the parent level (RFC 6962 promotion rule).
package merkle

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	// DefaultChunkSize is 32 KiB, matching croc's wire chunk size (TCP_BUFFER_SIZE / 2).
	DefaultChunkSize = 32 * 1024

	// HashSize is the byte length of a SHA-256 digest.
	HashSize = sha256.Size

	leafDomainPrefix byte = 0x00
	nodeDomainPrefix byte = 0x01
)

// Hash represents a 32-byte SHA-256 Merkle node or leaf digest.
type Hash = [HashSize]byte

var (
	// ErrInvalidChunkSize indicates a non-positive chunk size parameter.
	ErrInvalidChunkSize = errors.New("merkle: chunk size must be positive")
	// ErrInvalidFileSize indicates a negative file size parameter.
	ErrInvalidFileSize = errors.New("merkle: file size cannot be negative")
	// ErrInvalidPackedHashes indicates a byte slice whose length is not a multiple of 32.
	ErrInvalidPackedHashes = errors.New("merkle: packed leaf hashes length must be a multiple of 32")
	// ErrLeafCountMismatch indicates that the number of leaves does not match fileSize and chunkSize.
	ErrLeafCountMismatch = errors.New("merkle: leaf count does not match file size and chunk size")

	emptyRoot = sha256.Sum256([]byte{leafDomainPrefix})
)

// EmptyRoot returns the canonical Merkle root for a 0-byte file: SHA256(0x00).
func EmptyRoot() Hash {
	return emptyRoot
}

// LeafHash computes the domain-separated SHA-256 digest of a single chunk:
// SHA256(0x00 || chunk).
func LeafHash(chunk []byte) Hash {
	h := sha256.New()
	var prefix [1]byte
	prefix[0] = leafDomainPrefix
	_, _ = h.Write(prefix[:])
	_, _ = h.Write(chunk)
	var sum Hash
	h.Sum(sum[:0])
	return sum
}

// NodeHash computes the domain-separated SHA-256 digest of an internal node:
// SHA256(0x01 || left || right).
func NodeHash(left, right Hash) Hash {
	var buf [1 + HashSize + HashSize]byte
	buf[0] = nodeDomainPrefix
	copy(buf[1:1+HashSize], left[:])
	copy(buf[1+HashSize:], right[:])
	return sha256.Sum256(buf[:])
}

// ExpectedLeafCount returns the number of chunk leaves for fileSize at chunkSize.
func ExpectedLeafCount(fileSize int64, chunkSize int) (int, error) {
	if chunkSize <= 0 {
		return 0, ErrInvalidChunkSize
	}
	if fileSize < 0 {
		return 0, ErrInvalidFileSize
	}
	if fileSize == 0 {
		return 0, nil
	}
	return int((fileSize + int64(chunkSize) - 1) / int64(chunkSize)), nil
}

// ComputeRoot reduces a slice of leaf hashes bottom-up into a single Merkle root.
// It does not mutate the caller's slice.
func ComputeRoot(leaves []Hash) Hash {
	if len(leaves) == 0 {
		return EmptyRoot()
	}
	if len(leaves) == 1 {
		return leaves[0]
	}

	level := make([]Hash, len(leaves))
	copy(level, leaves)

	for len(level) > 1 {
		nextIdx := 0
		for i := 0; i+1 < len(level); i += 2 {
			level[nextIdx] = NodeHash(level[i], level[i+1])
			nextIdx++
		}
		if len(level)%2 == 1 {
			level[nextIdx] = level[len(level)-1]
			nextIdx++
		}
		level = level[:nextIdx]
	}
	return level[0]
}

// Tree holds the Merkle root and leaf digests of a file partitioned into fixed-size chunks.
type Tree struct {
	root      Hash
	leaves    []Hash
	chunkSize int
	fileSize  int64 // -1 when constructed solely from leaf hashes without known byte size
}

// BuildTreeFromFile reads the file at path using a parallel worker pool and constructs its Merkle tree.
func BuildTreeFromFile(path string, chunkSize int) (*Tree, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return BuildTreeFromOpenedFile(f, chunkSize)
}

// BuildTreeFromOpenedFile builds a Merkle tree from an already-open *os.File without
// altering its seek offset.
func BuildTreeFromOpenedFile(f *os.File, chunkSize int) (*Tree, error) {
	if f == nil {
		return nil, errors.New("merkle: nil file")
	}
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	leafCount, err := ExpectedLeafCount(fileSize, chunkSize)
	if err != nil {
		return nil, err
	}
	if leafCount == 0 {
		return &Tree{
			root:      EmptyRoot(),
			chunkSize: chunkSize,
			fileSize:  0,
		}, nil
	}

	leaves := make([]Hash, leafCount)
	workers := min(max(1, runtime.NumCPU()), leafCount)

	if workers == 1 {
		buf := make([]byte, chunkSize)
		for idx := range leafCount {
			offset := int64(idx) * int64(chunkSize)
			want := int(min(int64(chunkSize), fileSize-offset))
			n, readErr := readFullAt(f, buf[:want], offset)
			if readErr != nil {
				return nil, fmt.Errorf("merkle: read chunk %d: %w", idx, readErr)
			}
			leaves[idx] = LeafHash(buf[:n])
		}
	} else {
		var nextIdx atomic.Int64
		var firstErr atomic.Pointer[error]
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				buf := make([]byte, chunkSize)
				for {
					idx := int(nextIdx.Add(1) - 1)
					if idx >= leafCount || firstErr.Load() != nil {
						return
					}
					offset := int64(idx) * int64(chunkSize)
					want := int(min(int64(chunkSize), fileSize-offset))
					n, readErr := readFullAt(f, buf[:want], offset)
					if readErr != nil {
						wrapped := fmt.Errorf("merkle: read chunk %d: %w", idx, readErr)
						firstErr.CompareAndSwap(nil, &wrapped)
						return
					}
					leaves[idx] = LeafHash(buf[:n])
				}
			}()
		}
		wg.Wait()
		if errPtr := firstErr.Load(); errPtr != nil {
			return nil, *errPtr
		}
	}

	return &Tree{
		root:      ComputeRoot(leaves),
		leaves:    leaves,
		chunkSize: chunkSize,
		fileSize:  fileSize,
	}, nil
}

// BuildTreeFromBytes constructs a Merkle tree directly from an in-memory byte slice.
func BuildTreeFromBytes(data []byte, chunkSize int) (*Tree, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	fileSize := int64(len(data))
	leafCount, err := ExpectedLeafCount(fileSize, chunkSize)
	if err != nil {
		return nil, err
	}
	if leafCount == 0 {
		return &Tree{
			root:      EmptyRoot(),
			chunkSize: chunkSize,
			fileSize:  0,
		}, nil
	}
	leaves := make([]Hash, leafCount)
	for idx := range leafCount {
		start := idx * chunkSize
		end := min(start+chunkSize, len(data))
		leaves[idx] = LeafHash(data[start:end])
	}
	return &Tree{
		root:      ComputeRoot(leaves),
		leaves:    leaves,
		chunkSize: chunkSize,
		fileSize:  fileSize,
	}, nil
}

// NewTreeFromLeaves reconstructs a Tree from remote leaf hashes (e.g. from MerkleManifest).
func NewTreeFromLeaves(leaves []Hash, fileSize int64, chunkSize int) (*Tree, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	if fileSize >= 0 {
		expected, err := ExpectedLeafCount(fileSize, chunkSize)
		if err != nil {
			return nil, err
		}
		if len(leaves) != expected {
			return nil, fmt.Errorf("%w: got %d leaves for %d bytes (chunkSize=%d, want %d)",
				ErrLeafCountMismatch, len(leaves), fileSize, chunkSize, expected)
		}
	}
	var copied []Hash
	if len(leaves) > 0 {
		copied = make([]Hash, len(leaves))
		copy(copied, leaves)
	}
	return &Tree{
		root:      ComputeRoot(copied),
		leaves:    copied,
		chunkSize: chunkSize,
		fileSize:  fileSize,
	}, nil
}

// Root returns the 32-byte Merkle root of the tree.
func (t *Tree) Root() Hash {
	if t == nil {
		return EmptyRoot()
	}
	return t.root
}

// LeafHashes returns a defensive copy of the tree's leaf hashes.
func (t *Tree) LeafHashes() []Hash {
	if t == nil || len(t.leaves) == 0 {
		return nil
	}
	out := make([]Hash, len(t.leaves))
	copy(out, t.leaves)
	return out
}

// LeafHashAt returns the leaf digest at index without allocating a slice copy.
func (t *Tree) LeafHashAt(index int) (Hash, bool) {
	if t == nil || index < 0 || index >= len(t.leaves) {
		return Hash{}, false
	}
	return t.leaves[index], true
}

// LeafCount returns the number of chunk leaves in the tree.
func (t *Tree) LeafCount() int {
	if t == nil {
		return 0
	}
	return len(t.leaves)
}

// ChunkSize returns the chunk size in bytes used to build the tree.
func (t *Tree) ChunkSize() int {
	if t == nil {
		return 0
	}
	return t.chunkSize
}

// FileSize returns the file byte size if known, or -1 if unspecified.
func (t *Tree) FileSize() int64 {
	if t == nil {
		return 0
	}
	return t.fileSize
}

// VerifyChunk checks whether data matches the expected leaf hash (and expected byte length,
// when FileSize is known) for chunk index.
func (t *Tree) VerifyChunk(index int, data []byte) bool {
	if t == nil || index < 0 || index >= len(t.leaves) {
		return false
	}
	if len(data) == 0 || len(data) > t.chunkSize {
		return false
	}
	if t.fileSize >= 0 {
		offset := int64(index) * int64(t.chunkSize)
		expectedLen := int(min(int64(t.chunkSize), t.fileSize-offset))
		if len(data) != expectedLen {
			return false
		}
	}
	return LeafHash(data) == t.leaves[index]
}

// DiffLeaves compares localLeaves against remoteLeaves using DefaultChunkSize (32 KiB)
// and returns mismatched or missing leaf spans encoded in croc's ChunkRanges format:
// [chunkSize, startByteOffset0, chunkCount0, startByteOffset1, chunkCount1, ...].
// If every leaf in remoteLeaves matches localLeaves, it returns nil.
func DiffLeaves(localLeaves, remoteLeaves []Hash) []int64 {
	return DiffLeavesChunkSize(localLeaves, remoteLeaves, DefaultChunkSize)
}

// DiffLeavesChunkSize is DiffLeaves with an explicit chunkSize in bytes.
func DiffLeavesChunkSize(localLeaves, remoteLeaves []Hash, chunkSize int) []int64 {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if len(remoteLeaves) == 0 {
		return nil
	}

	var ranges []int64
	var runStart, runCount int64
	step := int64(chunkSize)

	flush := func() {
		if runCount == 0 {
			return
		}
		if len(ranges) == 0 {
			ranges = append(ranges, step)
		}
		ranges = append(ranges, runStart, runCount)
		runCount = 0
	}

	for i := range remoteLeaves {
		mismatch := i >= len(localLeaves) || localLeaves[i] != remoteLeaves[i]
		if mismatch {
			if runCount == 0 {
				runStart = int64(i) * step
			}
			runCount++
		} else {
			flush()
		}
	}
	flush()
	return ranges
}

// PackHashes serializes a slice of 32-byte hashes into a contiguous byte slice.
func PackHashes(hashes []Hash) []byte {
	if len(hashes) == 0 {
		return nil
	}
	out := make([]byte, len(hashes)*HashSize)
	for i, h := range hashes {
		copy(out[i*HashSize:(i+1)*HashSize], h[:])
	}
	return out
}

// UnpackHashes deserializes a contiguous byte slice into 32-byte hashes.
func UnpackHashes(packed []byte) ([]Hash, error) {
	if len(packed) == 0 {
		return nil, nil
	}
	if len(packed)%HashSize != 0 {
		return nil, fmt.Errorf("%w: got %d bytes", ErrInvalidPackedHashes, len(packed))
	}
	count := len(packed) / HashSize
	out := make([]Hash, count)
	for i := range count {
		copy(out[i][:], packed[i*HashSize:(i+1)*HashSize])
	}
	return out, nil
}

// readFullAt reads exactly len(buf) bytes from r starting at off.
func readFullAt(r io.ReaderAt, buf []byte, off int64) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.ReadAt(buf[total:], off+int64(total))
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) && total < len(buf) {
				return total, io.ErrUnexpectedEOF
			}
			if total == len(buf) && errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}
