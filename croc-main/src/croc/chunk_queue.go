package croc

import (
	"context"
	"errors"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/utils"
)

// requestedChunkQueue manages sender chunk distribution across parallel TCP
// workers. For small-file bundles (progress != nil) or direct legacy tests it
// provides sequential range claiming; for regular file transfers (progress == nil)
// it delegates to ChunkScheduler + StreamTracker (SCH-01..SCH-05).
type requestedChunkQueue struct {
	workers           sync.WaitGroup
	compressed        bool
	progress          func(int64, int)
	mu                sync.Mutex
	ranges            []int64
	next              int   // index of the current range start in ranges
	step              int64 // chunks already claimed from the current range
	total             int
	completed         int
	completedByWorker map[int]int
	done              sync.Once
	onDone            func()

	scheduler     ChunkScheduler
	tracker       *StreamTracker
	unitCtx       context.Context
	unitCancel    context.CancelFunc
	activeWorkers atomic.Int32
	sentPackets   atomic.Int64
	unitFinished  atomic.Bool
	closeFileOnce sync.Once
	closeFileFn   func()
}

func newRequestedChunkQueue(ranges []int64, fileSize, chunkSize int64, onDone func()) *requestedChunkQueue {
	if len(ranges) == 0 {
		ranges = []int64{chunkSize, 0, int64(utils.ChunkRangesCount(nil, fileSize, chunkSize))}
	}
	queue := &requestedChunkQueue{ranges: ranges, next: 1, onDone: onDone}
	for i := 2; i < len(ranges); i += 2 {
		if ranges[i] > 0 {
			queue.total += int(ranges[i])
		}
	}
	if queue.total == 0 {
		queue.signalDone()
	}
	return queue
}

func (q *requestedChunkQueue) claim() (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for ; q.next+1 < len(q.ranges); q.next += 2 {
		if q.step < q.ranges[q.next+1] {
			offset := q.ranges[q.next] + q.step*q.ranges[0]
			q.step++
			return offset, true
		}
		q.step = 0
	}
	return 0, false
}

func (q *requestedChunkQueue) complete() {
	q.mu.Lock()
	finished := q.completeLocked()
	q.mu.Unlock()
	if finished {
		q.signalDone()
	}
}

func (q *requestedChunkQueue) completeLocked() bool {
	q.completed++
	return q.completed == q.total
}

func (q *requestedChunkQueue) signalDone() {
	q.done.Do(func() {
		if q.onDone != nil {
			q.onDone()
		}
	})
}

func (q *requestedChunkQueue) isUnitFinished() bool {
	return q.unitFinished.Load()
}

func (q *requestedChunkQueue) closeFileSafe() {
	q.closeFileOnce.Do(func() {
		if q.closeFileFn != nil {
			q.closeFileFn()
		}
	})
}

func (q *requestedChunkQueue) cancelUnit() {
	if q.unitCancel != nil {
		q.unitCancel()
	}
}

func (q *requestedChunkQueue) stopTracker() {
	if q.tracker != nil {
		q.tracker.Stop()
	}
}

func (q *requestedChunkQueue) acquireTicket(workerID int) (ChunkTicket, bool) {
	if q.scheduler != nil {
		return q.scheduler.Acquire(q.unitCtx, workerID)
	}
	offset, ok := q.claim()
	return ChunkTicket{Offset: offset}, ok
}

func (q *requestedChunkQueue) recordPacketSent() {
	q.sentPackets.Add(1)
}

func (q *requestedChunkQueue) recordWorkerFirstCompletionLocked(workerID int) {
	if q.completedByWorker == nil {
		q.completedByWorker = make(map[int]int)
	}
	q.completedByWorker[workerID]++
}

func (q *requestedChunkQueue) workerChunksSnapshot() map[int]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.completedByWorker) == 0 {
		return map[int]int{}
	}
	return maps.Clone(q.completedByWorker)
}

func (q *requestedChunkQueue) completeTicket(workerID int, ticket ChunkTicket, bytesSent int, sendDur time.Duration) bool {
	if q.scheduler == nil {
		q.mu.Lock()
		q.recordWorkerFirstCompletionLocked(workerID)
		finished := q.completeLocked()
		q.mu.Unlock()
		if finished {
			q.signalDone()
		}
		return true
	}
	first := q.scheduler.Complete(workerID, ticket, bytesSent, sendDur)
	if first {
		q.mu.Lock()
		q.completed++
		q.recordWorkerFirstCompletionLocked(workerID)
		q.mu.Unlock()
	}
	return first
}

func (q *requestedChunkQueue) releaseTicket(workerID int, ticket ChunkTicket) {
	if q.scheduler != nil {
		q.scheduler.Release(workerID, ticket)
	}
}

func (c *Client) startSenderChunkQueue(attempt *transferAttemptState, file *os.File) {
	c.startSenderUnit(attempt, file, c.FilesToTransfer[c.FilesToTransferCurrentNum].Size,
		c.currentFileUsesCompression(), c.CurrentFileChunkRanges, nil)
}

func (c *Client) startSenderUnit(
	attempt *transferAttemptState,
	file *os.File,
	size int64,
	compressed bool,
	ranges []int64,
	progress func(int64, int),
) {
	closeFile := func() {
		if file == nil {
			return
		}
		log.Debug("closing file")
		if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			log.Errorf("error closing file: %v", err)
		}
	}

	var queue *requestedChunkQueue
	if progress != nil {
		queue = newRequestedChunkQueue(ranges, size, models.TCP_BUFFER_SIZE/2, closeFile)
	} else {
		queue = c.newScheduledSenderQueue(attempt, size, ranges, closeFile)
	}
	queue.compressed = compressed
	queue.progress = progress

	c.senderDataMu.Lock()
	c.senderChunkQueue = queue
	c.senderDataAttempt = attempt
	c.senderDataFile = file
	c.senderDataWorkers = make(map[*comm.Comm]struct{})
	c.senderWorkerSequence = 0
	c.senderDataMu.Unlock()

	connections := c.connectionsSnapshot()
	if len(connections) > 0 {
		connections = connections[1:]
	}
	for i, connection := range connections {
		if connection != nil {
			log.Debugf("starting sending over comm %d", i)
			c.startLateSenderWorker(connection, attempt)
		}
	}
}

func (c *Client) newScheduledSenderQueue(
	attempt *transferAttemptState,
	size int64,
	ranges []int64,
	closeFile func(),
) *requestedChunkQueue {
	baseCtx := c.clientContext()
	if attempt != nil && attempt.ctx != nil {
		baseCtx = attempt.ctx
	}
	unitCtx, unitCancel := context.WithCancel(baseCtx)
	tracker := NewStreamTracker(c.metrics)
	tracker.Start(unitCtx)

	queue := &requestedChunkQueue{
		tracker:     tracker,
		unitCtx:     unitCtx,
		unitCancel:  unitCancel,
		closeFileFn: closeFile,
	}

	mode := c.effectiveSchedMode()
	sched := newChunkSchedulerByMode(mode, ranges, size, models.TCP_BUFFER_SIZE/2, tracker, func() {
		queue.unitFinished.Store(true)
		unitCancel()
		if queue.activeWorkers.Load() == 0 {
			queue.closeFileSafe()
		}
	})
	queue.scheduler = sched
	queue.total = sched.Stats().TotalChunks
	return queue
}

func (c *Client) startLateSenderWorker(dataConn *comm.Comm, expectedAttempt *transferAttemptState) {
	if dataConn == nil {
		return
	}
	c.senderDataMu.Lock()
	queue, file, attempt := c.senderChunkQueue, c.senderDataFile, c.senderDataAttempt
	if queue == nil || file == nil || attempt == nil || attempt != expectedAttempt {
		c.senderDataMu.Unlock()
		return
	}
	if _, exists := c.senderDataWorkers[dataConn]; exists {
		c.senderDataMu.Unlock()
		return
	}
	c.senderDataWorkers[dataConn] = struct{}{}
	workerID := c.senderWorkerSequence
	c.senderWorkerSequence++
	queue.activeWorkers.Add(1)
	queue.workers.Add(1)
	tracker := queue.tracker
	c.senderDataMu.Unlock()

	if tracker != nil {
		tracker.RegisterWorkerConn(workerID, dataConn)
	}
	go func() {
		defer func() {
			if queue.activeWorkers.Add(-1) == 0 && queue.unitFinished.Load() {
				queue.closeFileSafe()
			}
			queue.workers.Done()
		}()
		c.sendData(workerID, dataConn, file, queue, attempt)
	}()
}

// Detach under the same lock used to admit late workers, then join every
// admitted worker before a new payload may use these untagged data frames.
func (c *Client) joinSenderUnit() int64 {
	c.senderDataMu.Lock()
	queue := c.senderChunkQueue
	c.senderChunkQueue, c.senderDataFile, c.senderDataAttempt = nil, nil, nil
	c.senderDataMu.Unlock()
	if queue == nil {
		return 0
	}
	queue.cancelUnit()
	queue.workers.Wait()
	queue.stopTracker()
	queue.closeFileSafe()

	snapshot := queue.workerChunksSnapshot()
	c.senderDataMu.Lock()
	c.lastWorkerChunks = snapshot
	c.senderDataMu.Unlock()
	return queue.sentPackets.Load()
}

// LastSenderWorkerChunks returns an immutable copy of per-worker first-completed
// chunk counts from the most recently finished sender file unit (SCH-05).
func (c *Client) LastSenderWorkerChunks() map[int]int {
	c.senderDataMu.Lock()
	defer c.senderDataMu.Unlock()
	if c.senderChunkQueue != nil {
		return c.senderChunkQueue.workerChunksSnapshot()
	}
	if len(c.lastWorkerChunks) == 0 {
		return map[int]int{}
	}
	return maps.Clone(c.lastWorkerChunks)
}

// ReenqueueRanges schedules corrupted or missing chunk ranges (in standard
// croc ChunkRanges format: [chunkSize, start1, count1, ...]) onto the active
// sender scheduler for selective retransmission (SCH-04 / MRK-05).
func (c *Client) ReenqueueRanges(ranges []int64) int {
	c.senderDataMu.Lock()
	queue := c.senderChunkQueue
	c.senderDataMu.Unlock()
	if queue == nil || queue.scheduler == nil {
		return 0
	}
	enqueued := queue.scheduler.ReenqueueRanges(ranges)
	if enqueued > 0 {
		st := queue.scheduler.Stats()
		queue.mu.Lock()
		queue.total = st.TotalChunks
		queue.completed = st.CompletedChunks
		queue.mu.Unlock()
	}
	return enqueued
}
