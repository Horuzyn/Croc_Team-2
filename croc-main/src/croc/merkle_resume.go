package croc

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/merkle"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/telemetry"
	"github.com/schollz/croc/v11/src/utils"
)

const (
	merkleManifestLeavesPerBatch = 4096 // 4096 * 32B = 128 KiB per control message
	maxMerkleRepairRounds        = 3
)

type merkleManifestPayload struct {
	FileIndex  int    `json:"fi"`
	FileSize   int64  `json:"fs"`
	ChunkSize  int    `json:"cs"`
	LeafCount  int    `json:"lc"`
	Root       []byte `json:"r"`
	StartLeaf  int    `json:"sl"`
	LeafHashes []byte `json:"lh,omitempty"`
	Final      bool   `json:"fn"`
}

type chunkRepairPayload struct {
	FileIndex int     `json:"fi"`
	Round     int     `json:"rd"`
	Ranges    []int64 `json:"rg"`
}

type incomingMerkleManifest struct {
	fileSize  int64
	chunkSize int
	leafCount int
	root      merkle.Hash
	leaves    []merkle.Hash
}

type merkleTransferState struct {
	mu             sync.Mutex
	incoming       map[int]*incomingMerkleManifest
	trees          map[int]*merkle.Tree
	corruptChunks  map[int]struct{}
	repairRound    int
	lastScan       merkle.ResumeScanResult
	hasLastScan    bool
	repairAcksRecv int
}

func (c *Client) merkleResumeActive() bool {
	return c.Options.MerkleResume && c.peerMerkleChunkResume
}

func (c *Client) resetMerkleState() {
	c.merkle.mu.Lock()
	defer c.merkle.mu.Unlock()
	c.merkle.incoming = nil
	c.merkle.trees = nil
	c.merkle.corruptChunks = nil
	c.merkle.repairRound = 0
	c.merkle.lastScan = merkle.ResumeScanResult{}
	c.merkle.hasLastScan = false
	c.merkle.repairAcksRecv = 0
}

func (c *Client) resetMerkleFileState() {
	c.merkle.mu.Lock()
	defer c.merkle.mu.Unlock()
	c.merkle.corruptChunks = nil
	c.merkle.repairRound = 0
	c.merkle.lastScan = merkle.ResumeScanResult{}
	c.merkle.hasLastScan = false
}

func (c *Client) merkleTreeForFile(fileIndex int) *merkle.Tree {
	c.merkle.mu.Lock()
	defer c.merkle.mu.Unlock()
	if c.merkle.trees == nil {
		return nil
	}
	return c.merkle.trees[fileIndex]
}

// sendMerkleManifestsForTransfer computes chunk-level SHA-256 Merkle trees for all
// non-empty files in c.FilesToTransfer, populates FileInfo.MerkleRoot, and streams
// TypeMerkleManifest batches to the peer before SenderInfo is sent.
func (c *Client) sendMerkleManifestsForTransfer() error {
	if !c.merkleResumeActive() {
		return nil
	}
	const chunkSize = models.TCP_BUFFER_SIZE / 2
	for i := range c.FilesToTransfer {
		fi := &c.FilesToTransfer[i]
		if fi.Size <= 0 || fi.Symlink != "" {
			emptyRoot := merkle.EmptyRoot()
			fi.MerkleRoot = slices.Clone(emptyRoot[:])
			continue
		}
		fpath := path.Join(fi.FolderSource, fi.Name)
		tree, err := merkle.BuildTreeFromFile(fpath, chunkSize)
		if err != nil {
			return fmt.Errorf("build merkle tree for %s: %w", fpath, err)
		}
		root := tree.Root()
		fi.MerkleRoot = slices.Clone(root[:])

		leaves := tree.LeafHashes()
		for start := 0; start < len(leaves); start += merkleManifestLeavesPerBatch {
			end := min(start+merkleManifestLeavesPerBatch, len(leaves))
			payload, err := json.Marshal(merkleManifestPayload{
				FileIndex:  i,
				FileSize:   fi.Size,
				ChunkSize:  chunkSize,
				LeafCount:  len(leaves),
				Root:       root[:],
				StartLeaf:  start,
				LeafHashes: merkle.PackHashes(leaves[start:end]),
				Final:      end == len(leaves),
			})
			if err != nil {
				return err
			}
			if err := message.Send(c.connection(0), c.Key, message.Message{
				Type:  message.TypeMerkleManifest,
				Bytes: payload,
			}); err != nil {
				return err
			}
		}
		log.Debugf("sent merkle manifest for file %d (%s): %d leaves, root=%x", i, fi.Name, len(leaves), root[:8])
	}
	return nil
}

func (c *Client) processMessageMerkleManifest(m message.Message) error {
	if c.Options.IsSender {
		return errors.New("sender received unexpected merkle-manifest")
	}
	if !c.merkleResumeActive() {
		return errors.New("peer sent merkle-manifest without merkle-chunk-resume-v1")
	}
	if len(c.Key) == 0 || !c.lifecycleSnapshot().ChannelSecured {
		return errors.New("peer sent merkle-manifest before secure channel")
	}

	var p merkleManifestPayload
	if err := json.Unmarshal(m.Bytes, &p); err != nil {
		return fmt.Errorf("decode merkle-manifest: %w", err)
	}
	if p.FileIndex < 0 || p.FileSize < 0 || p.ChunkSize <= 0 || p.LeafCount < 0 {
		return errors.New("invalid merkle-manifest metadata")
	}
	if len(p.Root) != merkle.HashSize {
		return fmt.Errorf("invalid merkle root length %d", len(p.Root))
	}
	expectedLeaves, err := merkle.ExpectedLeafCount(p.FileSize, p.ChunkSize)
	if err != nil || p.LeafCount != expectedLeaves {
		return fmt.Errorf("merkle-manifest leaf count mismatch: got %d, want %d", p.LeafCount, expectedLeaves)
	}
	batchLeaves, err := merkle.UnpackHashes(p.LeafHashes)
	if err != nil {
		return err
	}

	var root merkle.Hash
	copy(root[:], p.Root)

	c.merkle.mu.Lock()
	defer c.merkle.mu.Unlock()
	if c.merkle.incoming == nil {
		c.merkle.incoming = make(map[int]*incomingMerkleManifest)
	}
	acc, exists := c.merkle.incoming[p.FileIndex]
	if !exists {
		if p.StartLeaf != 0 {
			return fmt.Errorf("merkle-manifest batch started at leaf %d, want 0", p.StartLeaf)
		}
		acc = &incomingMerkleManifest{
			fileSize:  p.FileSize,
			chunkSize: p.ChunkSize,
			leafCount: p.LeafCount,
			root:      root,
			leaves:    make([]merkle.Hash, 0, p.LeafCount),
		}
		c.merkle.incoming[p.FileIndex] = acc
	} else {
		if acc.fileSize != p.FileSize || acc.chunkSize != p.ChunkSize || acc.leafCount != p.LeafCount || acc.root != root {
			delete(c.merkle.incoming, p.FileIndex)
			return errors.New("merkle-manifest header changed mid-stream")
		}
		if p.StartLeaf != len(acc.leaves) {
			delete(c.merkle.incoming, p.FileIndex)
			return fmt.Errorf("unexpected merkle-manifest start leaf %d, want %d", p.StartLeaf, len(acc.leaves))
		}
	}
	if len(acc.leaves)+len(batchLeaves) > acc.leafCount {
		delete(c.merkle.incoming, p.FileIndex)
		return errors.New("merkle-manifest exceeded declared leaf count")
	}
	acc.leaves = append(acc.leaves, batchLeaves...)

	if !p.Final {
		return nil
	}
	if len(acc.leaves) != acc.leafCount {
		delete(c.merkle.incoming, p.FileIndex)
		return fmt.Errorf("incomplete merkle-manifest: got %d/%d leaves", len(acc.leaves), acc.leafCount)
	}
	tree, err := merkle.NewTreeFromLeaves(acc.leaves, acc.fileSize, acc.chunkSize)
	delete(c.merkle.incoming, p.FileIndex)
	if err != nil {
		return err
	}
	if tree.Root() != acc.root {
		return fmt.Errorf("merkle-manifest root mismatch: computed %x != declared %x", tree.Root(), acc.root)
	}
	if c.merkle.trees == nil {
		c.merkle.trees = make(map[int]*merkle.Tree)
	}
	c.merkle.trees[p.FileIndex] = tree
	log.Debugf("received merkle manifest for file %d (%d leaves, root=%x)", p.FileIndex, tree.LeafCount(), acc.root[:8])
	return nil
}

// verifiedResumeRanges runs parallel Merkle chunk verification for recipientInitializeFile
// when merkle-chunk-resume-v1 is active and a remote Merkle tree is available.
func (c *Client) verifiedResumeRanges(file *os.File, fileIndex int, fileSize int64) ([]int64, bool) {
	if !c.merkleResumeActive() {
		return nil, false
	}
	tree := c.merkleTreeForFile(fileIndex)
	if tree == nil {
		return nil, false
	}
	res, err := merkle.ScanResumeFile(file, fileSize, models.TCP_BUFFER_SIZE/2, tree.LeafHashes())
	if err != nil {
		log.Debugf("merkle resume scan failed (%v); falling back to zero-scan", err)
		return nil, false
	}
	c.merkle.mu.Lock()
	c.merkle.lastScan = res
	c.merkle.hasLastScan = true
	c.merkle.mu.Unlock()

	log.Debugf("merkle resume scan: valid=%d missing=%d corrupt=%d retransmitted=%d saved=%d",
		res.ValidChunks, res.MissingChunks, res.CorruptChunks, res.RetransmittedBytes, res.SavedBytes)
	if res.CorruptChunks > 0 || res.SavedBytes > 0 {
		c.metrics.Emit(telemetry.MerkleVerify{
			CorruptChunks:      res.CorruptChunks,
			RetransmittedBytes: res.RetransmittedBytes,
			SavedBytes:         res.SavedBytes,
		})
	}
	return res.Ranges, true
}

// verifyIncomingChunk validates a received plaintext chunk against the remote Merkle tree.
// Corrupted chunks are recorded for selective repair at the end of the pass.
func (c *Client) verifyIncomingChunk(position int64, plaintext []byte) {
	if !c.merkleResumeActive() {
		return
	}
	tree := c.merkleTreeForFile(c.FilesToTransferCurrentNum)
	if tree == nil {
		return
	}
	chunkSize := tree.ChunkSize()
	if chunkSize <= 0 {
		chunkSize = models.TCP_BUFFER_SIZE / 2
	}
	chunkIdx := int(position / int64(chunkSize))
	valid := tree.VerifyChunk(chunkIdx, plaintext)

	c.merkle.mu.Lock()
	if valid {
		if c.merkle.corruptChunks != nil {
			delete(c.merkle.corruptChunks, chunkIdx)
		}
	} else {
		if c.merkle.corruptChunks == nil {
			c.merkle.corruptChunks = make(map[int]struct{})
		}
		c.merkle.corruptChunks[chunkIdx] = struct{}{}
		log.Debugf("merkle chunk verification failed at index %d (offset %d)", chunkIdx, position)
	}
	c.merkle.mu.Unlock()
}

// finishOrRequestMerkleRepair checks whether any received chunks failed inline Merkle
// verification at the end of a receive pass. If repair is needed and allowed, it updates
// CurrentFileChunkCount/CurrentFileChunkRanges and returns repairRanges != nil.
func (c *Client) finishOrRequestMerkleRepair(fileSize int64) (repairRanges []int64, round int, err error) {
	if !c.merkleResumeActive() {
		return nil, 0, nil
	}
	tree := c.merkleTreeForFile(c.FilesToTransferCurrentNum)
	if tree == nil {
		return nil, 0, nil
	}

	c.merkle.mu.Lock()
	corruptCount := len(c.merkle.corruptChunks)
	if corruptCount == 0 {
		saved := int64(0)
		if c.merkle.hasLastScan {
			saved = c.merkle.lastScan.SavedBytes
		}
		c.merkle.mu.Unlock()
		c.metrics.Emit(telemetry.MerkleVerify{
			CorruptChunks:      0,
			RetransmittedBytes: 0,
			SavedBytes:         saved,
		})
		return nil, 0, nil
	}
	if c.merkle.repairRound >= maxMerkleRepairRounds {
		c.merkle.mu.Unlock()
		return nil, 0, fmt.Errorf("merkle chunk verification failed after %d repair rounds (%d corrupt chunks remain)", maxMerkleRepairRounds, corruptCount)
	}
	c.merkle.repairRound++
	round = c.merkle.repairRound

	// Build sorted ChunkRanges for all corrupted chunk indices.
	localLeaves := tree.LeafHashes()
	for idx := range c.merkle.corruptChunks {
		if idx >= 0 && idx < len(localLeaves) {
			localLeaves[idx] = merkle.Hash{} // force mismatch against remoteLeaves
		}
	}
	remoteLeaves := tree.LeafHashes()
	// Restore remote leaf at idx? Wait: localLeaves was a copy, so zeroing idx
	// guarantees localLeaves[idx] != remoteLeaves[idx] unless remote leaf is all-zero
	// (probability 2^-256; flip 1 bit to be 100% deterministic).
	for idx := range c.merkle.corruptChunks {
		if idx >= 0 && idx < len(localLeaves) {
			localLeaves[idx][0] ^= 0xFF
		}
	}
	saved := int64(0)
	if c.merkle.hasLastScan {
		saved = c.merkle.lastScan.SavedBytes
	}
	c.merkle.mu.Unlock()

	chunkSize := models.TCP_BUFFER_SIZE / 2
	repairRanges = merkle.DiffLeavesChunkSize(localLeaves, remoteLeaves, chunkSize)
	retransmitted := utils.ChunkRangesBytes(repairRanges, fileSize, int64(chunkSize))
	c.metrics.Emit(telemetry.MerkleVerify{
		CorruptChunks:      corruptCount,
		RetransmittedBytes: retransmitted,
		SavedBytes:         saved,
	})
	return repairRanges, round, nil
}

func (c *Client) sendChunkRepairRequest(fileIndex, round int, ranges []int64) error {
	payload, err := json.Marshal(chunkRepairPayload{
		FileIndex: fileIndex,
		Round:     round,
		Ranges:    ranges,
	})
	if err != nil {
		return err
	}
	log.Debugf("requesting selective repair for file %d round %d (%d chunks)",
		fileIndex, round, utils.ChunkRangesCount(ranges, c.FilesToTransfer[fileIndex].Size, models.TCP_BUFFER_SIZE/2))
	return message.Send(c.connection(0), c.Key, message.Message{
		Type:  message.TypeChunkRepairRequest,
		Bytes: payload,
	})
}

func (c *Client) processMessageChunkRepairRequest(m message.Message, attempt *transferAttemptState) error {
	if !c.Options.IsSender {
		return errors.New("recipient received unexpected chunk-repair-req")
	}
	if !c.merkleResumeActive() {
		return errors.New("peer sent chunk-repair-req without merkle-chunk-resume-v1")
	}
	var p chunkRepairPayload
	if err := json.Unmarshal(m.Bytes, &p); err != nil {
		return fmt.Errorf("decode chunk-repair-req: %w", err)
	}
	if p.FileIndex != c.FilesToTransferCurrentNum || p.FileIndex < 0 || p.FileIndex >= len(c.FilesToTransfer) {
		return fmt.Errorf("invalid chunk-repair-req file index %d", p.FileIndex)
	}
	if p.Round <= 0 || p.Round > maxMerkleRepairRounds {
		return fmt.Errorf("invalid chunk-repair-req round %d", p.Round)
	}
	fileInfo := c.FilesToTransfer[p.FileIndex]
	if err := validateRepairRanges(p.Ranges, fileInfo.Size, models.TCP_BUFFER_SIZE/2); err != nil {
		return err
	}

	if err := message.Send(c.connection(0), c.Key, message.Message{
		Type:  message.TypeChunkRepairAck,
		Bytes: m.Bytes,
	}); err != nil {
		return err
	}

	// Wait for any previous sender unit workers to exit cleanly, then re-enqueue
	// the requested repair ranges for transmission.
	c.joinSenderUnit()
	pathToFile := path.Join(fileInfo.FolderSource, fileInfo.Name)
	f, err := os.Open(pathToFile)
	if err != nil {
		return err
	}
	c.CurrentFileChunkRanges = p.Ranges
	c.CurrentFileChunkCount = utils.ChunkRangesCount(p.Ranges, fileInfo.Size, models.TCP_BUFFER_SIZE/2)
	c.startSenderUnit(attempt, f, fileInfo.Size, c.currentFileUsesCompression(), p.Ranges, nil)
	log.Debugf("sender re-enqueued %d repair chunks for file %d round %d", c.CurrentFileChunkCount, p.FileIndex, p.Round)
	return nil
}

func (c *Client) processMessageChunkRepairAck(m message.Message) error {
	if c.Options.IsSender {
		return errors.New("sender received unexpected chunk-repair-ack")
	}
	if !c.merkleResumeActive() {
		return errors.New("peer sent chunk-repair-ack without merkle-chunk-resume-v1")
	}
	var p chunkRepairPayload
	if err := json.Unmarshal(m.Bytes, &p); err != nil {
		return fmt.Errorf("decode chunk-repair-ack: %w", err)
	}
	c.merkle.mu.Lock()
	c.merkle.repairAcksRecv++
	c.merkle.mu.Unlock()
	log.Debugf("received chunk-repair-ack for file %d round %d", p.FileIndex, p.Round)
	return nil
}

func validateRepairRanges(ranges []int64, fileSize, defaultChunkSize int64) error {
	if len(ranges) < 3 || len(ranges)%2 == 0 {
		return errors.New("invalid chunk-repair-req ranges format")
	}
	chunkSize := ranges[0]
	if chunkSize != defaultChunkSize {
		return fmt.Errorf("invalid repair chunk size %d, want %d", chunkSize, defaultChunkSize)
	}
	for i := 1; i+1 < len(ranges); i += 2 {
		start, count := ranges[i], ranges[i+1]
		if start < 0 || count <= 0 || start%chunkSize != 0 || start >= fileSize {
			return fmt.Errorf("invalid repair range [%d, %d)", start, count)
		}
		if count > (fileSize-start+chunkSize-1)/chunkSize {
			return fmt.Errorf("repair range exceeds file size: start=%d count=%d size=%d", start, count, fileSize)
		}
	}
	return nil
}
