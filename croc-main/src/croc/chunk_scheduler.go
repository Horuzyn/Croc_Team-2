package croc

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/telemetry"
	"github.com/schollz/croc/v11/src/utils"
)

const (
	// FeatureDynamicChunkSchedV1 aliases the shared PAKE feature flag from message.
	FeatureDynamicChunkSchedV1 = message.FeatureDynamicChunkSchedV1

	// SchedModeStatic preserves croc's sequential range claiming without work-stealing.
	SchedModeStatic = "static"
	// SchedModeDynamic enables per-stream throughput tracking, slow-stream pacing, and work-stealing.
	SchedModeDynamic = "dynamic"

	// pagedBitsetBitsPerWord is 64 bits per uint64 word.
	pagedBitsetBitsPerWord = 64
	// pagedBitsetWordsPerPage is 512 words (4 KiB), covering 32,768 chunks (~1 GiB at 32 KiB/chunk).
	pagedBitsetWordsPerPage = 512
	pagedBitsetBitsPerPage  = pagedBitsetBitsPerWord * pagedBitsetWordsPerPage

	// MaxConcurrentChunkOwners caps how many workers may hold the same chunk simultaneously
	// (1 primary worker + at most 1 work-stealing worker) to prevent congestion collapse.
	MaxConcurrentChunkOwners = 2
)

// ChunkState represents the lifecycle state of a file chunk in the scheduler.
type ChunkState uint8

const (
	ChunkStatePending ChunkState = iota
	ChunkStateInFlight
	ChunkStateDone
)

func (s ChunkState) String() string {
	switch s {
	case ChunkStatePending:
		return "Pending"
	case ChunkStateInFlight:
		return "InFlight"
	case ChunkStateDone:
		return "Done"
	default:
		return "Unknown"
	}
}

// WorkerTier classifies a TCP worker's relative throughput within the sender pool.
type WorkerTier uint8

const (
	WorkerNormal WorkerTier = iota
	WorkerFast
	WorkerSlow
)

func (t WorkerTier) String() string {
	switch t {
	case WorkerNormal:
		return "Normal"
	case WorkerFast:
		return "Fast"
	case WorkerSlow:
		return "Slow"
	default:
		return "Unknown"
	}
}

// StreamSpeedInspector decouples ChunkScheduler (SCH-01/SCH-03) from StreamTracker (SCH-02)
// and telemetry sinks (INT-01). Implementations must never call back into ChunkScheduler
// so lock ordering remains strictly one-way.
type StreamSpeedInspector interface {
	ClassifyWorker(workerID int) WorkerTier
	MedianChunkDuration() time.Duration
	RecordSendSample(workerID int, bytes int, sendDur time.Duration)
	RecordSteal(thiefWorkerID int)
}

// ChunkTicket represents a worker's lease to transmit the chunk at Offset.
type ChunkTicket struct {
	Offset   int64
	IsStolen bool
}

// SchedulerStats exposes a point-in-time snapshot of scheduler counters for
// testing and JSONL telemetry reporting.
type SchedulerStats struct {
	TotalChunks     int
	CompletedChunks int
	PendingChunks   int
	InFlightChunks  int
	StolenChunks    uint64
	RequeuedChunks  uint64
}

// ChunkScheduler defines the unified scheduling contract for multi-stream TCP
// data workers, supporting lazy range claiming, speculative work-stealing, and
// selective chunk repair (MRK-05).
//
// Concurrency contract: implementations must never perform network/disk I/O,
// sleep, or invoke external callbacks (onDone, StreamSpeedInspector) while
// holding internal mutexes.
type ChunkScheduler interface {
	// Acquire returns the next chunk ticket for workerID, waiting if needed
	// until work becomes available, all chunks complete, or ctx is canceled.
	Acquire(ctx context.Context, workerID int) (ChunkTicket, bool)

	// Claim performs a non-blocking attempt to pop a Pending chunk for workerID.
	Claim(workerID int) (ChunkTicket, bool)

	// Steal performs a non-blocking attempt to duplicate an overdue InFlight
	// chunk onto a faster worker.
	Steal(workerID int) (ChunkTicket, bool)

	// Complete marks ticket.Offset as Done and reports send duration outside locks.
	// Returns firstCompletion=true iff this call transitioned the chunk to Done.
	Complete(workerID int, ticket ChunkTicket, bytesSent int, sendDur time.Duration) (firstCompletion bool)

	// Release returns an uncompleted InFlight chunk back to the Pending requeue
	// FIFO when a worker encounters an I/O or socket error before completion.
	Release(workerID int, ticket ChunkTicket)

	// ReenqueueRanges schedules corrupted or missing chunk ranges (in standard
	// croc ChunkRanges format: [chunkSize, startOffset1, count1, ...]) for
	// selective retransmission. Returns the number of chunks enqueued.
	ReenqueueRanges(ranges []int64) int

	// Stats returns a thread-safe snapshot of current scheduler metrics.
	Stats() SchedulerStats
}

// inflightChunkState tracks active ownership of a single chunk currently in transit.
type inflightChunkState struct {
	offset           int64
	primaryWorkerID  int
	primaryStartedAt time.Time
	hasPrimary       bool
	stolen           bool
	stealerWorkerID  int
	stealStartedAt   time.Time
	hasStealer       bool
}

func newInflightChunkState(offset int64, workerID int, now time.Time) *inflightChunkState {
	return &inflightChunkState{
		offset:           offset,
		primaryWorkerID:  workerID,
		primaryStartedAt: now,
		hasPrimary:       true,
	}
}

func (e *inflightChunkState) activeOwners() int {
	owners := 0
	if e.hasPrimary {
		owners++
	}
	if e.hasStealer {
		owners++
	}
	return owners
}

func (e *inflightChunkState) canBeStolenBy(workerID int) bool {
	if e.stolen || e.hasStealer || e.activeOwners() >= MaxConcurrentChunkOwners {
		return false
	}
	if e.hasPrimary && e.primaryWorkerID == workerID {
		return false
	}
	return true
}

func (e *inflightChunkState) attachStealer(workerID int, now time.Time) {
	e.stolen = true
	e.hasStealer = true
	e.stealerWorkerID = workerID
	e.stealStartedAt = now
}

func (e *inflightChunkState) detachWorker(workerID int, wasStolenTicket bool) (remaining int) {
	if wasStolenTicket && e.hasStealer && e.stealerWorkerID == workerID {
		e.hasStealer = false
		e.stolen = false
		return e.activeOwners()
	}
	if e.hasPrimary && e.primaryWorkerID == workerID {
		e.hasPrimary = false
		return e.activeOwners()
	}
	if e.hasStealer && e.stealerWorkerID == workerID {
		e.hasStealer = false
		e.stolen = false
	}
	return e.activeOwners()
}

func (e *inflightChunkState) oldestElapsed(now time.Time) time.Duration {
	if e.hasPrimary {
		return now.Sub(e.primaryStartedAt)
	}
	if e.hasStealer {
		return now.Sub(e.stealStartedAt)
	}
	return 0
}

// pagedChunkBitset tracks chunk indices in lazily allocated 4 KiB pages so
// initializing a 100 GiB transfer allocates 0 bytes upfront while providing O(1)
// lookup, set, and clear operations. Unaligned synthetic test offsets fall back
// to a small map allocated only on demand.
type pagedChunkBitset struct {
	chunkSize int64
	pages     map[uint64]*[pagedBitsetWordsPerPage]uint64
	unaligned map[int64]struct{}
}

func newPagedChunkBitset(chunkSize int64) pagedChunkBitset {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	return pagedChunkBitset{chunkSize: chunkSize}
}

func (b *pagedChunkBitset) splitAligned(offset int64) (pageIdx uint64, wordIdx uint64, mask uint64, aligned bool) {
	if offset < 0 || b.chunkSize <= 0 || offset%b.chunkSize != 0 {
		return 0, 0, 0, false
	}
	idx := uint64(offset / b.chunkSize)
	pageIdx = idx / pagedBitsetBitsPerPage
	bitInPage := idx % pagedBitsetBitsPerPage
	wordIdx = bitInPage / pagedBitsetBitsPerWord
	mask = uint64(1) << (bitInPage % pagedBitsetBitsPerWord)
	return pageIdx, wordIdx, mask, true
}

func (b *pagedChunkBitset) Test(offset int64) bool {
	pageIdx, wordIdx, mask, aligned := b.splitAligned(offset)
	if !aligned {
		if b.unaligned == nil {
			return false
		}
		_, exists := b.unaligned[offset]
		return exists
	}
	if b.pages == nil {
		return false
	}
	page := b.pages[pageIdx]
	if page == nil {
		return false
	}
	return (page[wordIdx] & mask) != 0
}

// Set marks offset and returns true iff offset was not previously set.
func (b *pagedChunkBitset) Set(offset int64) bool {
	pageIdx, wordIdx, mask, aligned := b.splitAligned(offset)
	if !aligned {
		if b.unaligned == nil {
			b.unaligned = make(map[int64]struct{})
		}
		if _, exists := b.unaligned[offset]; exists {
			return false
		}
		b.unaligned[offset] = struct{}{}
		return true
	}
	if b.pages == nil {
		b.pages = make(map[uint64]*[pagedBitsetWordsPerPage]uint64)
	}
	page := b.pages[pageIdx]
	if page == nil {
		page = new([pagedBitsetWordsPerPage]uint64)
		b.pages[pageIdx] = page
	}
	if (page[wordIdx] & mask) != 0 {
		return false
	}
	page[wordIdx] |= mask
	return true
}

// Clear unmarks offset and returns true iff offset was previously set.
func (b *pagedChunkBitset) Clear(offset int64) bool {
	pageIdx, wordIdx, mask, aligned := b.splitAligned(offset)
	if !aligned {
		if b.unaligned == nil {
			return false
		}
		if _, exists := b.unaligned[offset]; !exists {
			return false
		}
		delete(b.unaligned, offset)
		return true
	}
	if b.pages == nil {
		return false
	}
	page := b.pages[pageIdx]
	if page == nil || (page[wordIdx]&mask) == 0 {
		return false
	}
	page[wordIdx] &^= mask
	return true
}

// lazyChunkRangeCursor generates chunk offsets on demand from croc's compact
// [chunkSize, start1, count1, ...] slice without materializing all offsets.
type lazyChunkRangeCursor struct {
	ranges    []int64
	chunkSize int64
	fileSize  int64
	next      int
	step      int64
	total     int
	remaining int
	sorted    bool
}

func newLazyChunkRangeCursor(ranges []int64, fileSize, defaultChunkSize int64) lazyChunkRangeCursor {
	chunkSize := defaultChunkSize
	if len(ranges) > 0 && ranges[0] > 0 {
		chunkSize = ranges[0]
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	if len(ranges) == 0 {
		if fileSize > 0 {
			ranges = []int64{chunkSize, 0, int64(utils.ChunkRangesCount(nil, fileSize, chunkSize))}
		} else {
			ranges = []int64{chunkSize}
		}
	}

	total := 0
	sorted := true
	var prevEnd int64
	hasPrev := false
	for i := 1; i+1 < len(ranges); i += 2 {
		start, count := ranges[i], ranges[i+1]
		if count <= 0 {
			continue
		}
		total += int(count)
		if hasPrev && start < prevEnd {
			sorted = false
		}
		prevEnd = start + count*chunkSize
		hasPrev = true
	}

	return lazyChunkRangeCursor{
		ranges:    ranges,
		chunkSize: chunkSize,
		fileSize:  fileSize,
		next:      1,
		total:     total,
		remaining: total,
		sorted:    sorted,
	}
}

func (c *lazyChunkRangeCursor) nextOffset() (int64, bool) {
	for ; c.next+1 < len(c.ranges); c.next += 2 {
		if c.step < c.ranges[c.next+1] {
			offset := c.ranges[c.next] + c.step*c.chunkSize
			c.step++
			c.remaining--
			return offset, true
		}
		c.step = 0
	}
	return 0, false
}

func (c *lazyChunkRangeCursor) containsInitialOffset(offset int64) bool {
	if offset < 0 || c.chunkSize <= 0 {
		return false
	}
	if c.sorted {
		if !utils.ChunkRangesContain(c.ranges, offset) {
			return false
		}
		// Verify alignment with the matching range start.
		pairs := (len(c.ranges) - 1) / 2
		low, high := 0, pairs
		for low < high {
			mid := low + (high-low)/2
			if c.ranges[1+mid*2] <= offset {
				low = mid + 1
			} else {
				high = mid
			}
		}
		if low == 0 {
			return false
		}
		start := c.ranges[1+(low-1)*2]
		return (offset-start)%c.chunkSize == 0
	}
	for i := 1; i+1 < len(c.ranges); i += 2 {
		start, count := c.ranges[i], c.ranges[i+1]
		if count <= 0 || offset < start {
			continue
		}
		if offset < start+count*c.chunkSize && (offset-start)%c.chunkSize == 0 {
			return true
		}
	}
	return false
}

// staticChunkScheduler implements ChunkScheduler with sequential range claiming
// (no speculative work-stealing) while supporting Release, ReenqueueRanges,
// idempotent completion accounting, and strict lock-free I/O boundaries.
type staticChunkScheduler struct {
	mu              sync.Mutex
	cursor          lazyChunkRangeCursor
	requeued        []int64
	requeuedPending pagedChunkBitset
	issuedSet       pagedChunkBitset
	completedSet    pagedChunkBitset
	extraTargets    pagedChunkBitset
	inFlight        map[int64]*inflightChunkState
	total           int
	completed       int
	requeuedTotal   uint64
	wakeupCh        chan struct{}

	inspector StreamSpeedInspector
	doneOnce  sync.Once
	onDone    func()
}

func newStaticChunkScheduler(
	ranges []int64,
	fileSize int64,
	chunkSize int64,
	inspector StreamSpeedInspector,
	onDone func(),
) *staticChunkScheduler {
	cursor := newLazyChunkRangeCursor(ranges, fileSize, chunkSize)
	effChunkSize := cursor.chunkSize
	s := &staticChunkScheduler{
		cursor:          cursor,
		requeuedPending: newPagedChunkBitset(effChunkSize),
		issuedSet:       newPagedChunkBitset(effChunkSize),
		completedSet:    newPagedChunkBitset(effChunkSize),
		extraTargets:    newPagedChunkBitset(effChunkSize),
		inFlight:        make(map[int64]*inflightChunkState),
		total:           cursor.total,
		wakeupCh:        make(chan struct{}),
		inspector:       inspector,
		onDone:          onDone,
	}
	if s.total == 0 {
		s.signalDone()
	}
	return s
}

func (s *staticChunkScheduler) broadcastLocked() {
	close(s.wakeupCh)
	s.wakeupCh = make(chan struct{})
}

func (s *staticChunkScheduler) signalDone() {
	s.doneOnce.Do(func() {
		if s.onDone != nil {
			s.onDone()
		}
	})
}

func (s *staticChunkScheduler) Acquire(ctx context.Context, workerID int) (ChunkTicket, bool) {
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return ChunkTicket{}, false
			}
		}

		s.mu.Lock()
		if ticket, ok := s.claimLocked(workerID, time.Now()); ok {
			s.mu.Unlock()
			return ticket, true
		}
		if s.isFinishedLocked() {
			s.mu.Unlock()
			return ChunkTicket{}, false
		}
		// All Pending chunks have been claimed, but some InFlight chunks are still
		// running (which might fail and call Release, or finish the transfer).
		waitCh := s.wakeupCh
		s.mu.Unlock()

		if ctx == nil {
			<-waitCh
			continue
		}
		select {
		case <-ctx.Done():
			return ChunkTicket{}, false
		case <-waitCh:
		}
	}
}

func (s *staticChunkScheduler) Claim(workerID int) (ChunkTicket, bool) {
	s.mu.Lock()
	ticket, ok := s.claimLocked(workerID, time.Now())
	s.mu.Unlock()
	return ticket, ok
}

func (s *staticChunkScheduler) claimLocked(workerID int, now time.Time) (ChunkTicket, bool) {
	for len(s.requeued) > 0 {
		offset := s.requeued[0]
		s.requeued[0] = 0
		s.requeued = s.requeued[1:]
		if len(s.requeued) == 0 {
			s.requeued = nil
		}
		s.requeuedPending.Clear(offset)
		if s.completedSet.Test(offset) {
			continue
		}
		if existing := s.inFlight[offset]; existing != nil {
			if !existing.hasPrimary {
				existing.hasPrimary = true
				existing.primaryWorkerID = workerID
				existing.primaryStartedAt = now
				return ChunkTicket{Offset: offset, IsStolen: false}, true
			}
			continue
		}
		s.issuedSet.Set(offset)
		s.inFlight[offset] = newInflightChunkState(offset, workerID, now)
		return ChunkTicket{Offset: offset, IsStolen: false}, true
	}

	for {
		offset, ok := s.cursor.nextOffset()
		if !ok {
			return ChunkTicket{}, false
		}
		s.issuedSet.Set(offset)
		if s.completedSet.Test(offset) {
			continue
		}
		s.inFlight[offset] = newInflightChunkState(offset, workerID, now)
		return ChunkTicket{Offset: offset, IsStolen: false}, true
	}
}

// Steal always returns false in static scheduling mode.
func (s *staticChunkScheduler) Steal(_ int) (ChunkTicket, bool) {
	return ChunkTicket{}, false
}

func (s *staticChunkScheduler) Complete(workerID int, ticket ChunkTicket, bytesSent int, sendDur time.Duration) bool {
	s.mu.Lock()
	if entry := s.inFlight[ticket.Offset]; entry != nil {
		if entry.detachWorker(workerID, ticket.IsStolen) == 0 {
			delete(s.inFlight, ticket.Offset)
		}
	}
	firstCompletion := s.completedSet.Set(ticket.Offset)
	if firstCompletion {
		s.completed++
	}
	finished := s.isFinishedLocked()
	if finished || len(s.inFlight) == 0 {
		s.broadcastLocked()
	}
	inspector := s.inspector
	s.mu.Unlock()

	// Strictly outside s.mu: report speed metrics and run completion callback.
	if inspector != nil {
		inspector.RecordSendSample(workerID, bytesSent, sendDur)
	}
	if finished {
		s.signalDone()
	}
	return firstCompletion
}

func (s *staticChunkScheduler) Release(workerID int, ticket ChunkTicket) {
	s.mu.Lock()
	entry := s.inFlight[ticket.Offset]
	remaining := 0
	if entry != nil {
		remaining = entry.detachWorker(workerID, ticket.IsStolen)
		if remaining == 0 {
			delete(s.inFlight, ticket.Offset)
		}
	}
	if remaining == 0 && !s.completedSet.Test(ticket.Offset) {
		if s.requeuedPending.Set(ticket.Offset) {
			s.requeued = append(s.requeued, ticket.Offset)
			s.requeuedTotal++
		}
	}
	s.broadcastLocked()
	s.mu.Unlock()
}

func (s *staticChunkScheduler) ReenqueueRanges(ranges []int64) int {
	if len(ranges) < 3 {
		return 0
	}
	rangeChunkSize := ranges[0]
	s.mu.Lock()
	if rangeChunkSize <= 0 {
		rangeChunkSize = s.cursor.chunkSize
	}
	enqueued := 0
	for i := 1; i+1 < len(ranges); i += 2 {
		start, count := ranges[i], ranges[i+1]
		if start < 0 || count <= 0 {
			continue
		}
		for j := int64(0); j < count; j++ {
			offset := start + j*rangeChunkSize
			if s.cursor.fileSize > 0 && offset >= s.cursor.fileSize {
				break
			}
			inTarget := s.cursor.containsInitialOffset(offset) || s.extraTargets.Test(offset)
			if !inTarget {
				s.extraTargets.Set(offset)
				s.issuedSet.Set(offset)
				s.total++
			} else if s.completedSet.Clear(offset) {
				s.completed--
			} else if !s.issuedSet.Test(offset) {
				// Still untouched in the lazy range cursor; will be claimed naturally.
				continue
			}

			if s.requeuedPending.Set(offset) {
				s.requeued = append(s.requeued, offset)
				s.requeuedTotal++
				enqueued++
			}
		}
	}
	if enqueued > 0 {
		s.broadcastLocked()
	}
	s.mu.Unlock()
	return enqueued
}

func (s *staticChunkScheduler) Stats() SchedulerStats {
	s.mu.Lock()
	stats := SchedulerStats{
		TotalChunks:     s.total,
		CompletedChunks: s.completed,
		PendingChunks:   s.cursor.remaining + len(s.requeued),
		InFlightChunks:  len(s.inFlight),
		StolenChunks:    0,
		RequeuedChunks:  s.requeuedTotal,
	}
	s.mu.Unlock()
	return stats
}

func (s *staticChunkScheduler) isFinishedLocked() bool {
	return s.completed == s.total && s.cursor.remaining == 0 && len(s.requeued) == 0
}

// AdaptiveSchedulerConfig controls pacing and work-stealing thresholds for
// adaptiveChunkScheduler. Zero values fall back to DefaultAdaptiveSchedulerConfig().
type AdaptiveSchedulerConfig struct {
	MidStealMultiplier       float64
	MinMidStealThreshold     time.Duration
	EndgameStealMultiplier   float64
	MinEndgameStealThreshold time.Duration
	SlowMinCooldown          time.Duration
	SlowMaxCooldown          time.Duration
	TailProtectionFactor     int
	WorkerActiveWindow       time.Duration
}

func DefaultAdaptiveSchedulerConfig() AdaptiveSchedulerConfig {
	return AdaptiveSchedulerConfig{
		MidStealMultiplier:       3.0,
		MinMidStealThreshold:     2 * time.Millisecond,
		EndgameStealMultiplier:   1.5,
		MinEndgameStealThreshold: 1 * time.Millisecond,
		SlowMinCooldown:          2 * time.Millisecond,
		SlowMaxCooldown:          25 * time.Millisecond,
		TailProtectionFactor:     2,
		WorkerActiveWindow:       2 * time.Second,
	}
}

func normalizeAdaptiveConfig(cfg AdaptiveSchedulerConfig) AdaptiveSchedulerConfig {
	def := DefaultAdaptiveSchedulerConfig()
	if cfg.MidStealMultiplier <= 0 {
		cfg.MidStealMultiplier = def.MidStealMultiplier
	}
	if cfg.MinMidStealThreshold <= 0 {
		cfg.MinMidStealThreshold = def.MinMidStealThreshold
	}
	if cfg.EndgameStealMultiplier <= 0 {
		cfg.EndgameStealMultiplier = def.EndgameStealMultiplier
	}
	if cfg.MinEndgameStealThreshold <= 0 {
		cfg.MinEndgameStealThreshold = def.MinEndgameStealThreshold
	}
	if cfg.SlowMinCooldown <= 0 {
		cfg.SlowMinCooldown = def.SlowMinCooldown
	}
	if cfg.SlowMaxCooldown <= 0 {
		cfg.SlowMaxCooldown = def.SlowMaxCooldown
	}
	if cfg.SlowMaxCooldown < cfg.SlowMinCooldown {
		cfg.SlowMaxCooldown = cfg.SlowMinCooldown
	}
	if cfg.TailProtectionFactor <= 0 {
		cfg.TailProtectionFactor = def.TailProtectionFactor
	}
	if cfg.WorkerActiveWindow <= 0 {
		cfg.WorkerActiveWindow = def.WorkerActiveWindow
	}
	return cfg
}

func medianDuration(durs []time.Duration) time.Duration {
	n := len(durs)
	if n == 0 {
		return 0
	}
	slices.Sort(durs)
	if n%2 == 1 {
		return durs[n/2]
	}
	return (durs[n/2-1] + durs[n/2]) / 2
}

type speedSnapshot struct {
	medianDur  time.Duration
	smallTiers [16]WorkerTier
	largeTiers map[int]WorkerTier
	maxID      int
}

func (s *speedSnapshot) tierOf(workerID int) WorkerTier {
	if workerID < 0 || workerID > s.maxID {
		return WorkerNormal
	}
	if workerID < len(s.smallTiers) {
		return s.smallTiers[workerID]
	}
	if s.largeTiers != nil {
		if t, ok := s.largeTiers[workerID]; ok {
			return t
		}
	}
	return WorkerNormal
}

// adaptiveChunkScheduler implements ChunkScheduler with real-time stream tiering,
// slow-stream cooldown + tail protection, and speculative work-stealing (SCH-03).
type adaptiveChunkScheduler struct {
	cfg         AdaptiveSchedulerConfig
	inspector   StreamSpeedInspector
	maxWorkerID atomic.Int32

	mu              sync.Mutex
	cursor          lazyChunkRangeCursor
	requeued        []int64
	requeuedPending pagedChunkBitset
	issuedSet       pagedChunkBitset
	completedSet    pagedChunkBitset
	extraTargets    pagedChunkBitset
	inFlight        map[int64]*inflightChunkState
	workerLastSeen  map[int]time.Time
	total           int
	completed       int
	stolenTotal     uint64
	requeuedTotal   uint64
	wakeupCh        chan struct{}

	doneOnce sync.Once
	onDone   func()
}

func newAdaptiveChunkScheduler(
	ranges []int64,
	fileSize int64,
	chunkSize int64,
	inspector StreamSpeedInspector,
	onDone func(),
	cfg ...AdaptiveSchedulerConfig,
) *adaptiveChunkScheduler {
	var resolvedCfg AdaptiveSchedulerConfig
	if len(cfg) > 0 {
		resolvedCfg = normalizeAdaptiveConfig(cfg[0])
	} else {
		resolvedCfg = DefaultAdaptiveSchedulerConfig()
	}
	if inspector == nil {
		inspector = NewStreamTracker(telemetry.Nop)
	}
	cursor := newLazyChunkRangeCursor(ranges, fileSize, chunkSize)
	effChunkSize := cursor.chunkSize
	s := &adaptiveChunkScheduler{
		cfg:             resolvedCfg,
		inspector:       inspector,
		cursor:          cursor,
		requeuedPending: newPagedChunkBitset(effChunkSize),
		issuedSet:       newPagedChunkBitset(effChunkSize),
		completedSet:    newPagedChunkBitset(effChunkSize),
		extraTargets:    newPagedChunkBitset(effChunkSize),
		inFlight:        make(map[int64]*inflightChunkState),
		workerLastSeen:  make(map[int]time.Time),
		total:           cursor.total,
		wakeupCh:        make(chan struct{}),
		onDone:          onDone,
	}
	s.maxWorkerID.Store(-1)
	if s.total == 0 {
		s.signalDone()
	}
	return s
}

// newChunkSchedulerByMode constructs either the dynamic work-stealing scheduler
// or the static sequential baseline scheduler according to mode.
func newChunkSchedulerByMode(
	mode string,
	ranges []int64,
	fileSize int64,
	chunkSize int64,
	inspector StreamSpeedInspector,
	onDone func(),
) ChunkScheduler {
	if mode == SchedModeStatic {
		return newStaticChunkScheduler(ranges, fileSize, chunkSize, inspector, onDone)
	}
	return newAdaptiveChunkScheduler(ranges, fileSize, chunkSize, inspector, onDone)
}

func (s *adaptiveChunkScheduler) observeWorkerID(workerID int) {
	if workerID < 0 {
		return
	}
	for {
		cur := s.maxWorkerID.Load()
		if int32(workerID) <= cur {
			return
		}
		if s.maxWorkerID.CompareAndSwap(cur, int32(workerID)) {
			return
		}
	}
}

// snapshotSpeed queries StreamSpeedInspector strictly outside s.mu so zero locks
// are nested between ChunkScheduler and StreamTracker.
func (s *adaptiveChunkScheduler) snapshotSpeed(workerID int) speedSnapshot {
	s.observeWorkerID(workerID)
	snap := speedSnapshot{maxID: int(s.maxWorkerID.Load())}
	if s.inspector == nil {
		return snap
	}
	snap.medianDur = s.inspector.MedianChunkDuration()
	limit := min(snap.maxID, len(snap.smallTiers)-1)
	for id := 0; id <= limit; id++ {
		snap.smallTiers[id] = s.inspector.ClassifyWorker(id)
	}
	if snap.maxID >= len(snap.smallTiers) {
		snap.largeTiers = make(map[int]WorkerTier, snap.maxID-len(snap.smallTiers)+1)
		for id := len(snap.smallTiers); id <= snap.maxID; id++ {
			snap.largeTiers[id] = s.inspector.ClassifyWorker(id)
		}
	}
	return snap
}

func (s *adaptiveChunkScheduler) broadcastLocked() {
	close(s.wakeupCh)
	s.wakeupCh = make(chan struct{})
}

func (s *adaptiveChunkScheduler) signalDone() {
	s.doneOnce.Do(func() {
		if s.onDone != nil {
			s.onDone()
		}
	})
}

func (s *adaptiveChunkScheduler) Acquire(ctx context.Context, workerID int) (ChunkTicket, bool) {
	didCooldown := false
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return ChunkTicket{}, false
			}
		}

		snap := s.snapshotSpeed(workerID)
		tier := snap.tierOf(workerID)
		now := time.Now()

		s.mu.Lock()
		s.workerLastSeen[workerID] = now
		if s.isFinishedLocked() {
			s.mu.Unlock()
			return ChunkTicket{}, false
		}

		healthyPeers := s.healthyPeerCountLocked(workerID, now, &snap)

		// Slow-stream pacing: apply a bounded cooldown once per Acquire cycle
		// before handing new work to a degraded connection, strictly outside s.mu.
		if !didCooldown && tier == WorkerSlow && healthyPeers > 0 && s.pendingCountLocked() > 0 {
			cooldown := s.computeSlowCooldown(snap.medianDur)
			waitCh := s.wakeupCh
			s.mu.Unlock()
			didCooldown = true
			if !waitWithWakeup(ctx, waitCh, cooldown) {
				return ChunkTicket{}, false
			}
			continue
		}

		// Priority 1: Requeued chunks (unassigned failed/repaired chunks) always go first.
		if ticket, ok := s.popRequeuedLocked(workerID, now); ok {
			s.mu.Unlock()
			return ticket, true
		}

		// Priority 2: Mid-transfer speculative steal if an InFlight chunk has stalled
		// past max(3 * median_chunk_time, MinMidStealThreshold).
		if tier != WorkerSlow || healthyPeers == 0 {
			if ticket, ok := s.stealLocked(workerID, now, &snap, true); ok {
				inspector := s.inspector
				s.mu.Unlock()
				if inspector != nil {
					inspector.RecordSteal(workerID)
				}
				return ticket, true
			}
		}

		// Priority 3: Claim fresh Pending chunk from lazy cursor (enforcing Tail Protection).
		if ticket, ok := s.claimCursorLocked(workerID, now, tier, healthyPeers); ok {
			s.mu.Unlock()
			return ticket, true
		}

		// Priority 4: Endgame work-stealing when Pending == 0.
		if s.pendingCountLocked() == 0 && (tier != WorkerSlow || healthyPeers == 0) {
			if ticket, ok := s.stealLocked(workerID, now, &snap, false); ok {
				inspector := s.inspector
				s.mu.Unlock()
				if inspector != nil {
					inspector.RecordSteal(workerID)
				}
				return ticket, true
			}
		}

		if s.isFinishedLocked() {
			s.mu.Unlock()
			return ChunkTicket{}, false
		}

		waitDur, useTimer := s.nextWaitDurationLocked(workerID, now, tier, healthyPeers, &snap)
		waitCh := s.wakeupCh
		s.mu.Unlock()

		if useTimer {
			if !waitWithWakeup(ctx, waitCh, waitDur) {
				return ChunkTicket{}, false
			}
		} else {
			if ctx == nil {
				<-waitCh
			} else {
				select {
				case <-ctx.Done():
					return ChunkTicket{}, false
				case <-waitCh:
				}
			}
		}
	}
}

func waitWithWakeup(ctx context.Context, waitCh <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	if ctx == nil {
		select {
		case <-waitCh:
			return true
		case <-timer.C:
			return true
		}
	}
	select {
	case <-ctx.Done():
		return false
	case <-waitCh:
		return true
	case <-timer.C:
		return true
	}
}

func (s *adaptiveChunkScheduler) Claim(workerID int) (ChunkTicket, bool) {
	snap := s.snapshotSpeed(workerID)
	tier := snap.tierOf(workerID)
	now := time.Now()

	s.mu.Lock()
	s.workerLastSeen[workerID] = now
	if ticket, ok := s.popRequeuedLocked(workerID, now); ok {
		s.mu.Unlock()
		return ticket, true
	}
	healthyPeers := s.healthyPeerCountLocked(workerID, now, &snap)
	ticket, ok := s.claimCursorLocked(workerID, now, tier, healthyPeers)
	s.mu.Unlock()
	return ticket, ok
}

func (s *adaptiveChunkScheduler) Steal(workerID int) (ChunkTicket, bool) {
	snap := s.snapshotSpeed(workerID)
	tier := snap.tierOf(workerID)
	now := time.Now()

	s.mu.Lock()
	s.workerLastSeen[workerID] = now
	healthyPeers := s.healthyPeerCountLocked(workerID, now, &snap)
	if tier == WorkerSlow && healthyPeers > 0 {
		s.mu.Unlock()
		return ChunkTicket{}, false
	}
	midOnly := s.pendingCountLocked() > 0
	ticket, ok := s.stealLocked(workerID, now, &snap, midOnly)
	inspector := s.inspector
	s.mu.Unlock()

	if ok && inspector != nil {
		inspector.RecordSteal(workerID)
	}
	return ticket, ok
}

func (s *adaptiveChunkScheduler) Complete(workerID int, ticket ChunkTicket, bytesSent int, sendDur time.Duration) bool {
	s.observeWorkerID(workerID)
	now := time.Now()

	s.mu.Lock()
	s.workerLastSeen[workerID] = now
	// Once any owner finishes the chunk, remove it from inFlight immediately so no
	// other worker attempts to steal an already-completed chunk.
	delete(s.inFlight, ticket.Offset)

	firstCompletion := s.completedSet.Set(ticket.Offset)
	if firstCompletion {
		s.completed++
	}
	finished := s.isFinishedLocked()
	if finished || s.pendingCountLocked() == 0 || len(s.inFlight) == 0 {
		s.broadcastLocked()
	}
	inspector := s.inspector
	s.mu.Unlock()

	// Strictly outside s.mu: report send sample and run onDone callback.
	if inspector != nil {
		inspector.RecordSendSample(workerID, bytesSent, sendDur)
	}
	if finished {
		s.signalDone()
	}
	return firstCompletion
}

func (s *adaptiveChunkScheduler) Release(workerID int, ticket ChunkTicket) {
	s.observeWorkerID(workerID)
	s.mu.Lock()
	if s.completedSet.Test(ticket.Offset) {
		delete(s.inFlight, ticket.Offset)
		s.mu.Unlock()
		return
	}
	entry := s.inFlight[ticket.Offset]
	remaining := 0
	if entry != nil {
		remaining = entry.detachWorker(workerID, ticket.IsStolen)
		if remaining == 0 {
			delete(s.inFlight, ticket.Offset)
		}
	}
	if remaining == 0 {
		if s.requeuedPending.Set(ticket.Offset) {
			s.requeued = append(s.requeued, ticket.Offset)
			s.requeuedTotal++
		}
	}
	s.broadcastLocked()
	s.mu.Unlock()
}

func (s *adaptiveChunkScheduler) ReenqueueRanges(ranges []int64) int {
	if len(ranges) < 3 {
		return 0
	}
	rangeChunkSize := ranges[0]
	s.mu.Lock()
	if rangeChunkSize <= 0 {
		rangeChunkSize = s.cursor.chunkSize
	}
	enqueued := 0
	for i := 1; i+1 < len(ranges); i += 2 {
		start, count := ranges[i], ranges[i+1]
		if start < 0 || count <= 0 {
			continue
		}
		for j := int64(0); j < count; j++ {
			offset := start + j*rangeChunkSize
			if s.cursor.fileSize > 0 && offset >= s.cursor.fileSize {
				break
			}
			inTarget := s.cursor.containsInitialOffset(offset) || s.extraTargets.Test(offset)
			if !inTarget {
				s.extraTargets.Set(offset)
				s.issuedSet.Set(offset)
				s.total++
			} else if s.completedSet.Clear(offset) {
				s.completed--
			} else if !s.issuedSet.Test(offset) {
				continue
			}

			if s.requeuedPending.Set(offset) {
				s.requeued = append(s.requeued, offset)
				s.requeuedTotal++
				enqueued++
			}
		}
	}
	if enqueued > 0 {
		s.broadcastLocked()
	}
	s.mu.Unlock()
	return enqueued
}

func (s *adaptiveChunkScheduler) Stats() SchedulerStats {
	s.mu.Lock()
	stats := SchedulerStats{
		TotalChunks:     s.total,
		CompletedChunks: s.completed,
		PendingChunks:   s.pendingCountLocked(),
		InFlightChunks:  len(s.inFlight),
		StolenChunks:    s.stolenTotal,
		RequeuedChunks:  s.requeuedTotal,
	}
	s.mu.Unlock()
	return stats
}

func (s *adaptiveChunkScheduler) pendingCountLocked() int {
	return s.cursor.remaining + len(s.requeued)
}

func (s *adaptiveChunkScheduler) isFinishedLocked() bool {
	return s.completed == s.total && s.cursor.remaining == 0 && len(s.requeued) == 0
}

func (s *adaptiveChunkScheduler) healthyPeerCountLocked(excludeWorkerID int, now time.Time, snap *speedSnapshot) int {
	count := 0
	for id, lastSeen := range s.workerLastSeen {
		if id == excludeWorkerID {
			continue
		}
		if now.Sub(lastSeen) > s.cfg.WorkerActiveWindow {
			continue
		}
		if snap.tierOf(id) != WorkerSlow {
			count++
		}
	}
	return count
}

func (s *adaptiveChunkScheduler) computeSlowCooldown(medianDur time.Duration) time.Duration {
	cd := medianDur / 2
	if cd < s.cfg.SlowMinCooldown {
		return s.cfg.SlowMinCooldown
	}
	if cd > s.cfg.SlowMaxCooldown {
		return s.cfg.SlowMaxCooldown
	}
	return cd
}

func (s *adaptiveChunkScheduler) popRequeuedLocked(workerID int, now time.Time) (ChunkTicket, bool) {
	for len(s.requeued) > 0 {
		offset := s.requeued[0]
		s.requeued[0] = 0
		s.requeued = s.requeued[1:]
		if len(s.requeued) == 0 {
			s.requeued = nil
		}
		s.requeuedPending.Clear(offset)
		if s.completedSet.Test(offset) {
			continue
		}
		if existing := s.inFlight[offset]; existing != nil {
			if !existing.hasPrimary {
				existing.hasPrimary = true
				existing.primaryWorkerID = workerID
				existing.primaryStartedAt = now
				return ChunkTicket{Offset: offset, IsStolen: false}, true
			}
			continue
		}
		s.issuedSet.Set(offset)
		s.inFlight[offset] = newInflightChunkState(offset, workerID, now)
		return ChunkTicket{Offset: offset, IsStolen: false}, true
	}
	return ChunkTicket{}, false
}

func (s *adaptiveChunkScheduler) claimCursorLocked(
	workerID int,
	now time.Time,
	tier WorkerTier,
	healthyPeers int,
) (ChunkTicket, bool) {
	if s.cursor.remaining == 0 {
		return ChunkTicket{}, false
	}
	// Tail Protection: reserve final chunks for healthy peers so a throttled stream
	// cannot grab the very last chunk and stall transfer completion.
	if tier == WorkerSlow && healthyPeers > 0 {
		tailGuard := s.cfg.TailProtectionFactor * healthyPeers
		if s.cursor.remaining <= tailGuard {
			return ChunkTicket{}, false
		}
	}

	for {
		offset, ok := s.cursor.nextOffset()
		if !ok {
			return ChunkTicket{}, false
		}
		s.issuedSet.Set(offset)
		if s.completedSet.Test(offset) {
			continue
		}
		s.inFlight[offset] = newInflightChunkState(offset, workerID, now)
		return ChunkTicket{Offset: offset, IsStolen: false}, true
	}
}

func (s *adaptiveChunkScheduler) midStealThreshold(medianDur time.Duration) time.Duration {
	scaled := time.Duration(float64(medianDur) * s.cfg.MidStealMultiplier)
	if scaled < s.cfg.MinMidStealThreshold {
		return s.cfg.MinMidStealThreshold
	}
	return scaled
}

func (s *adaptiveChunkScheduler) endgameStealThreshold(medianDur time.Duration) time.Duration {
	scaled := time.Duration(float64(medianDur) * s.cfg.EndgameStealMultiplier)
	if scaled < s.cfg.MinEndgameStealThreshold {
		return s.cfg.MinEndgameStealThreshold
	}
	return scaled
}

func (s *adaptiveChunkScheduler) stealLocked(
	workerID int,
	now time.Time,
	snap *speedSnapshot,
	midTransferOnly bool,
) (ChunkTicket, bool) {
	if len(s.inFlight) == 0 {
		return ChunkTicket{}, false
	}
	midThresh := s.midStealThreshold(snap.medianDur)
	endThresh := s.endgameStealThreshold(snap.medianDur)

	var best *inflightChunkState
	var bestElapsed time.Duration
	bestPrimarySlow := false

	for _, entry := range s.inFlight {
		if s.completedSet.Test(entry.offset) || !entry.canBeStolenBy(workerID) {
			continue
		}
		elapsed := entry.oldestElapsed(now)
		primarySlow := entry.hasPrimary && snap.tierOf(entry.primaryWorkerID) == WorkerSlow

		eligible := false
		if midTransferOnly {
			eligible = elapsed > midThresh
		} else {
			eligible = primarySlow || elapsed >= endThresh
		}
		if !eligible {
			continue
		}

		if best == nil ||
			(primarySlow && !bestPrimarySlow) ||
			(primarySlow == bestPrimarySlow && elapsed > bestElapsed) ||
			(primarySlow == bestPrimarySlow && elapsed == bestElapsed && entry.offset < best.offset) {
			best = entry
			bestElapsed = elapsed
			bestPrimarySlow = primarySlow
		}
	}

	if best == nil {
		return ChunkTicket{}, false
	}
	best.attachStealer(workerID, now)
	s.stolenTotal++
	return ChunkTicket{Offset: best.offset, IsStolen: true}, true
}

func (s *adaptiveChunkScheduler) nextWaitDurationLocked(
	workerID int,
	now time.Time,
	tier WorkerTier,
	healthyPeers int,
	snap *speedSnapshot,
) (time.Duration, bool) {
	if tier == WorkerSlow && healthyPeers > 0 {
		return s.cfg.SlowMaxCooldown, true
	}

	endThresh := s.endgameStealThreshold(snap.medianDur)
	var minRem time.Duration
	foundCandidate := false
	for _, entry := range s.inFlight {
		if s.completedSet.Test(entry.offset) || !entry.canBeStolenBy(workerID) {
			continue
		}
		rem := endThresh - entry.oldestElapsed(now)
		if rem < 500*time.Microsecond {
			rem = 500 * time.Microsecond
		}
		if !foundCandidate || rem < minRem {
			minRem = rem
			foundCandidate = true
		}
	}
	return minRem, foundCandidate
}
