package merkle

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

// ResumeScanResult summarizes a parallel Merkle verification scan of a partial or
// corrupted destination file on disk.
type ResumeScanResult struct {
	// Ranges contains the union (missing ∪ corrupted) in croc ChunkRanges format:
	// [chunkSize, startOffset0, count0, ...]. Empty/nil means 100% of chunks are valid.
	Ranges []int64
	// MissingChunks is the number of chunks absent on disk (beyond EOF or unwritten all-zero blocks).
	MissingChunks int
	// CorruptChunks is the number of non-zero chunks on disk whose SHA-256 leaf failed verification.
	CorruptChunks int
	// ValidChunks is the number of chunks on disk matching remoteLeaves.
	ValidChunks int
	// RetransmittedBytes is the exact byte count covered by Ranges that must be requested again.
	RetransmittedBytes int64
	// SavedBytes is the exact byte count already verified on disk (fileSize - RetransmittedBytes).
	SavedBytes int64
}

// VerifiedMissingChunks scans path in parallel against remoteLeaves and returns
// (missing ∪ corrupted) chunk spans in croc's ChunkRanges format.
// If path does not exist yet on disk, all chunks in remoteLeaves are returned as missing.
func VerifiedMissingChunks(path string, fileSize int64, chunkSize int, remoteLeaves []Hash) ([]int64, error) {
	res, err := ScanResumePath(path, fileSize, chunkSize, remoteLeaves)
	if err != nil {
		return nil, err
	}
	return res.Ranges, nil
}

// VerifiedMissingChunksFile scans an already-open *os.File in parallel using runtime.NumCPU()
// workers without modifying f's seek offset, returning (missing ∪ corrupted) ChunkRanges.
func VerifiedMissingChunksFile(f *os.File, fileSize int64, chunkSize int, remoteLeaves []Hash) ([]int64, error) {
	res, err := ScanResumeFile(f, fileSize, chunkSize, remoteLeaves)
	if err != nil {
		return nil, err
	}
	return res.Ranges, nil
}

// ScanResumePath opens path (handling os.ErrNotExist as 100% missing) and runs ScanResumeFile.
func ScanResumePath(path string, fileSize int64, chunkSize int, remoteLeaves []Hash) (ResumeScanResult, error) {
	leafCount, err := validateResumeInputs(fileSize, chunkSize, remoteLeaves)
	if err != nil {
		return ResumeScanResult{}, err
	}
	if leafCount == 0 {
		return ResumeScanResult{}, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fullMissingResult(fileSize, chunkSize, leafCount), nil
		}
		return ResumeScanResult{}, err
	}
	defer f.Close()
	return ScanResumeFileWorkers(f, fileSize, chunkSize, remoteLeaves, runtime.NumCPU())
}

// ScanResumeFile verifies an open *os.File in parallel with runtime.NumCPU() workers.
func ScanResumeFile(f *os.File, fileSize int64, chunkSize int, remoteLeaves []Hash) (ResumeScanResult, error) {
	return ScanResumeFileWorkers(f, fileSize, chunkSize, remoteLeaves, runtime.NumCPU())
}

// ScanResumeFileWorkers verifies an open *os.File using the specified worker count.
func ScanResumeFileWorkers(f *os.File, fileSize int64, chunkSize int, remoteLeaves []Hash, workers int) (ResumeScanResult, error) {
	if f == nil {
		return ResumeScanResult{}, errors.New("merkle: nil file")
	}
	leafCount, err := validateResumeInputs(fileSize, chunkSize, remoteLeaves)
	if err != nil {
		return ResumeScanResult{}, err
	}
	if leafCount == 0 {
		return ResumeScanResult{}, nil
	}

	stat, err := f.Stat()
	if err != nil {
		return ResumeScanResult{}, err
	}
	diskSize := stat.Size()
	if diskSize <= 0 {
		return fullMissingResult(fileSize, chunkSize, leafCount), nil
	}

	const (
		chunkValid   uint8 = 0
		chunkMissing uint8 = 1
		chunkCorrupt uint8 = 2
	)

	states := make([]uint8, leafCount)
	workers = min(max(1, workers), leafCount)

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
				if offset+int64(want) > diskSize {
					states[idx] = chunkMissing
					continue
				}

				n, readErr := readFullAt(f, buf[:want], offset)
				if readErr != nil {
					if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
						states[idx] = chunkMissing
						continue
					}
					wrapped := fmt.Errorf("merkle: scan chunk %d: %w", idx, readErr)
					firstErr.CompareAndSwap(nil, &wrapped)
					return
				}

				chunk := buf[:n]
				if n == want && LeafHash(chunk) == remoteLeaves[idx] {
					states[idx] = chunkValid
					continue
				}
				if isAllZero(chunk) {
					states[idx] = chunkMissing
				} else {
					states[idx] = chunkCorrupt
				}
			}
		}()
	}
	wg.Wait()

	if errPtr := firstErr.Load(); errPtr != nil {
		return ResumeScanResult{}, *errPtr
	}

	var res ResumeScanResult
	var runStart, runCount int64
	step := int64(chunkSize)

	flush := func() {
		if runCount == 0 {
			return
		}
		if len(res.Ranges) == 0 {
			res.Ranges = append(res.Ranges, step)
		}
		res.Ranges = append(res.Ranges, runStart, runCount)
		runCount = 0
	}

	for idx, st := range states {
		offset := int64(idx) * step
		chunkBytes := min(step, fileSize-offset)
		switch st {
		case chunkValid:
			flush()
			res.ValidChunks++
			res.SavedBytes += chunkBytes
		case chunkMissing:
			if runCount == 0 {
				runStart = offset
			}
			runCount++
			res.MissingChunks++
			res.RetransmittedBytes += chunkBytes
		case chunkCorrupt:
			if runCount == 0 {
				runStart = offset
			}
			runCount++
			res.CorruptChunks++
			res.RetransmittedBytes += chunkBytes
		}
	}
	flush()

	return res, nil
}

func validateResumeInputs(fileSize int64, chunkSize int, remoteLeaves []Hash) (int, error) {
	expected, err := ExpectedLeafCount(fileSize, chunkSize)
	if err != nil {
		return 0, err
	}
	if len(remoteLeaves) != expected {
		return 0, fmt.Errorf("%w: got %d leaves for %d bytes (chunkSize=%d, want %d)",
			ErrLeafCountMismatch, len(remoteLeaves), fileSize, chunkSize, expected)
	}
	return expected, nil
}

func fullMissingResult(fileSize int64, chunkSize int, leafCount int) ResumeScanResult {
	if leafCount == 0 || fileSize == 0 {
		return ResumeScanResult{}
	}
	return ResumeScanResult{
		Ranges:             []int64{int64(chunkSize), 0, int64(leafCount)},
		MissingChunks:      leafCount,
		RetransmittedBytes: fileSize,
	}
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
