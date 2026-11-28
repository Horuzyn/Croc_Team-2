package croc

import (
	"context"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/utils"
)

// callbackLockProbeInspector verifies that StreamSpeedInspector callbacks are
// invoked strictly outside ChunkScheduler's internal mutex (calling Stats() on
// the same goroutine would deadlock immediately if s.mu were still locked).
type callbackLockProbeInspector struct {
	sched   ChunkScheduler
	samples atomic.Int64
}

func (p *callbackLockProbeInspector) ClassifyWorker(_ int) WorkerTier {
	return WorkerNormal
}

func (p *callbackLockProbeInspector) MedianChunkDuration() time.Duration {
	return 5 * time.Millisecond
}

func (p *callbackLockProbeInspector) RecordSendSample(_ int, _ int, _ time.Duration) {
	if p.sched != nil {
		_ = p.sched.Stats()
	}
	p.samples.Add(1)
}

func (p *callbackLockProbeInspector) RecordSteal(_ int) {}

func TestPagedChunkBitsetAlignedAndUnaligned(t *testing.T) {
	const chunkSize = int64(32 << 10) // 32 KiB
	bs := newPagedChunkBitset(chunkSize)

	if bs.pages != nil || bs.unaligned != nil {
		t.Fatalf("expected zero eager map allocation on newPagedChunkBitset")
	}

	offsets := []int64{
		0,
		chunkSize,
		chunkSize * (pagedBitsetBitsPerPage - 1),
		chunkSize * pagedBitsetBitsPerPage, // page 1 boundary (~1 GiB)
		chunkSize * (pagedBitsetBitsPerPage * 99), // ~99 GiB offset
		17, // synthetic unaligned offset
	}

	for _, off := range offsets {
		if bs.Test(off) {
			t.Fatalf("offset %d unexpectedly set", off)
		}
		if !bs.Set(off) {
			t.Fatalf("first Set(%d) = false, want true", off)
		}
		if !bs.Test(off) {
			t.Fatalf("Test(%d) = false after Set", off)
		}
		if bs.Set(off) {
			t.Fatalf("second Set(%d) = true, want false (idempotent)", off)
		}
	}

	for _, off := range offsets {
		if !bs.Clear(off) {
			t.Fatalf("first Clear(%d) = false, want true", off)
		}
		if bs.Test(off) {
			t.Fatalf("Test(%d) = true after Clear", off)
		}
		if bs.Clear(off) {
			t.Fatalf("second Clear(%d) = true, want false", off)
		}
	}
}

func TestInflightChunkStateLifecycle(t *testing.T) {
	t0 := time.Now()
	st := newInflightChunkState(65536, 0, t0)

	if st.activeOwners() != 1 {
		t.Fatalf("activeOwners = %d, want 1", st.activeOwners())
	}
	if st.canBeStolenBy(0) {
		t.Fatalf("primary worker must not steal its own chunk")
	}
	if !st.canBeStolenBy(1) {
		t.Fatalf("worker 1 should be allowed to steal un-stolen chunk")
	}

	st.attachStealer(1, t0.Add(60*time.Millisecond))
	if st.activeOwners() != 2 {
		t.Fatalf("activeOwners after steal = %d, want 2", st.activeOwners())
	}
	if st.canBeStolenBy(2) {
		t.Fatalf("chunk already at MaxConcurrentChunkOwners must not be stolen again")
	}

	if rem := st.detachWorker(1, true); rem != 1 {
		t.Fatalf("remaining after stealer detach = %d, want 1", rem)
	}
	if rem := st.detachWorker(0, false); rem != 0 {
		t.Fatalf("remaining after primary detach = %d, want 0", rem)
	}
}

func TestChunkSchedulerStaticMatchesChunkRanges(t *testing.T) {
	for _, ranges := range [][]int64{
		{16},
		{16, 48, 4},
		{16, 32, 2, 80, 1},
		{16, 0, 0, 32, 2},
		{16, 0, -3, 32, 2},
		{16, 80, 1, 0, 2},
	} {
		var done atomic.Int32
		var sched ChunkScheduler = newStaticChunkScheduler(ranges, 128, 16, nil, func() {
			done.Add(1)
		})
		var got []int64
		for {
			ticket, ok := sched.Claim(0)
			if !ok {
				break
			}
			got = append(got, ticket.Offset)
			if !sched.Complete(0, ticket, 16, time.Millisecond) {
				t.Fatalf("expected firstCompletion=true for offset %d", ticket.Offset)
			}
		}
		if want := utils.ChunkRangesToChunks(ranges); !slices.Equal(got, want) {
			t.Fatalf("ranges %v: offsets = %v, want %v", ranges, got, want)
		}
		if done.Load() != 1 {
			t.Fatalf("ranges %v: onDone ran %d times, want 1", ranges, done.Load())
		}
	}
}

func TestChunkSchedulerConcurrentAcquireReleaseAndReenqueue(t *testing.T) {
	const (
		chunkSize = int64(32)
		fileSize  = int64(32 * 64) // 64 chunks total
	)

	// Start with a partial resume covering only the second half [1024..2048) (32 chunks).
	initialRanges := []int64{chunkSize, 32 * 32, 32}

	var doneCalls atomic.Int32
	sched := newStaticChunkScheduler(initialRanges, fileSize, chunkSize, nil, func() {
		doneCalls.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Claim one chunk and simulate a transient socket error via Release().
	firstTicket, ok := sched.Claim(0)
	if !ok {
		t.Fatalf("expected initial Claim to succeed")
	}
	sched.Release(0, firstTicket)

	// Claim and complete 4 chunks, then revoke 2 of them + inject 3 repair chunks
	// from the first half of the file (simulating Long's MRK-05 Merkle repair).
	var completedEarly []int64
	for range 4 {
		tk, claimed := sched.Claim(1)
		if !claimed {
			t.Fatalf("expected Claim to succeed")
		}
		completedEarly = append(completedEarly, tk.Offset)
		if !sched.Complete(1, tk, int(chunkSize), time.Millisecond) {
			t.Fatalf("expected first completion for offset %d", tk.Offset)
		}
	}

	repairRanges := []int64{
		chunkSize,
		completedEarly[0], 2, // 2 chunks previously completed -> must be revoked and resent
		chunkSize * 2, 3, // 3 chunks outside initialRanges -> total grows from 32 to 35
	}
	if n := sched.ReenqueueRanges(repairRanges); n != 5 {
		t.Fatalf("ReenqueueRanges enqueued %d chunks, want 5", n)
	}
	if st := sched.Stats(); st.TotalChunks != 35 {
		t.Fatalf("TotalChunks = %d, want 35", st.TotalChunks)
	}

	// Drain remaining transfer across 6 concurrent workers using Acquire().
	var deliveries sync.Map
	var firstCompletions atomic.Int32
	firstCompletions.Add(2) // 4 early completions minus 2 revoked by ReenqueueRanges

	var wg sync.WaitGroup
	for workerID := range 6 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				tk, acquired := sched.Acquire(ctx, id)
				if !acquired {
					return
				}
				deliveries.Store(tk.Offset, true)
				if sched.Complete(id, tk, int(chunkSize), 200*time.Microsecond) {
					firstCompletions.Add(1)
				}
			}
		}(workerID)
	}
	wg.Wait()

	st := sched.Stats()
	if st.CompletedChunks != 35 || st.InFlightChunks != 0 || st.PendingChunks != 0 {
		t.Fatalf("unexpected final stats: %+v", st)
	}
	if firstCompletions.Load() != 35 {
		t.Fatalf("net firstCompletions = %d, want 35", firstCompletions.Load())
	}
	if doneCalls.Load() != 1 {
		t.Fatalf("onDone called %d times, want 1", doneCalls.Load())
	}

	// Verify the 3 out-of-band Merkle repair chunks were all delivered.
	for _, off := range []int64{chunkSize * 2, chunkSize * 3, chunkSize * 4} {
		if _, ok := deliveries.Load(off); !ok {
			t.Fatalf("repair chunk at offset %d was never delivered", off)
		}
	}
}

func TestChunkSchedulerCallbacksDoNotHoldMutex(t *testing.T) {
	probe := &callbackLockProbeInspector{}
	var onDoneStats SchedulerStats
	var sched *staticChunkScheduler
	sched = newStaticChunkScheduler(nil, 128, 32, probe, func() {
		// Re-entering sched.Stats() on the same goroutine proves s.mu is unlocked.
		onDoneStats = sched.Stats()
	})
	probe.sched = sched

	for {
		tk, ok := sched.Claim(0)
		if !ok {
			break
		}
		sched.Complete(0, tk, 32, time.Millisecond)
	}

	if probe.samples.Load() != 4 {
		t.Fatalf("recorded %d samples, want 4", probe.samples.Load())
	}
	if onDoneStats.CompletedChunks != 4 {
		t.Fatalf("onDone observed CompletedChunks=%d, want 4", onDoneStats.CompletedChunks)
	}
}

func TestChunkSchedulerLarge100GiBDoesNotMaterializeOffsets(t *testing.T) {
	const (
		chunkSize = int64(models.TCP_BUFFER_SIZE / 2)
		fileSize  = int64(100 << 30) // 100 GiB
	)
	sched := newStaticChunkScheduler(nil, fileSize, chunkSize, nil, nil)
	if len(sched.core.cursor.ranges) != 3 {
		t.Fatalf("expected 3 range entries for 100 GiB file, got %d", len(sched.core.cursor.ranges))
	}
	if sched.core.completedSet.pages != nil || sched.core.issuedSet.pages != nil {
		t.Fatalf("expected zero bitset pages allocated at startup for 100 GiB file")
	}
}

func BenchmarkStaticChunkSchedulerNew100GiB(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		newStaticChunkScheduler(nil, 100<<30, models.TCP_BUFFER_SIZE/2, nil, nil)
	}
}

func BenchmarkAdaptiveChunkSchedulerNew100GiB(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		newAdaptiveChunkScheduler(nil, 100<<30, models.TCP_BUFFER_SIZE/2, nil, nil)
	}
}

// runSimulated4WorkerPool executes a full mock transfer across 4 workers where
// workers 0..2 take fastDelay per chunk and worker 3 takes slowDelay (10x slower),
// mirroring croc's sender unit lifecycle where completion closes the transfer attempt.
func runSimulated4WorkerPool(
	t *testing.T,
	mode string,
	totalChunks int,
	fastDelay time.Duration,
	slowDelay time.Duration,
) (elapsed time.Duration, fastFirstCompletions int, stats SchedulerStats) {
	t.Helper()
	const chunkSize = int64(32 << 10)
	fileSize := chunkSize * int64(totalChunks)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var doneCalls atomic.Int32
	var doneAt atomic.Int64
	start := time.Now()

	sched := newChunkSchedulerByMode(mode, nil, fileSize, chunkSize, nil, func() {
		doneCalls.Add(1)
		doneAt.Store(int64(time.Since(start)))
		// Just like Receiver's TypeCloseSender in croc, cancel in-flight stuck sockets
		// once 100% of file chunks have been delivered.
		cancel()
	})

	var delivered sync.Map
	var firstCount atomic.Int32
	var fastFirst atomic.Int32

	var wg sync.WaitGroup
	for workerID := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			delay := fastDelay
			if id == 3 {
				delay = slowDelay
			}
			for {
				tk, ok := sched.Acquire(ctx, id)
				if !ok {
					return
				}
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
				delivered.Store(tk.Offset, true)
				if sched.Complete(id, tk, int(chunkSize), delay) {
					firstCount.Add(1)
					if id != 3 {
						fastFirst.Add(1)
					}
				}
			}
		}(workerID)
	}
	wg.Wait()

	if doneCalls.Load() != 1 {
		t.Fatalf("[%s] onDone called %d times, want 1", mode, doneCalls.Load())
	}
	if int(firstCount.Load()) != totalChunks {
		t.Fatalf("[%s] firstCompletions = %d, want %d", mode, firstCount.Load(), totalChunks)
	}
	for i := range totalChunks {
		off := int64(i) * chunkSize
		if _, ok := delivered.Load(off); !ok {
			t.Fatalf("[%s] missing chunk at offset %d", mode, off)
		}
	}

	return time.Duration(doneAt.Load()), int(fastFirst.Load()), sched.Stats()
}

// TestAdaptiveChunkScheduler4WorkersWith1Slow10x validates the primary DoD of SCH-03:
// 4 workers where 1 worker is 10x slower must finish markedly faster under dynamic
// work-stealing than static claiming, with 100% chunk delivery and zero deadlock/livelock.
func TestAdaptiveChunkScheduler4WorkersWith1Slow10x(t *testing.T) {
	const (
		totalChunks = 38
		fastDelay   = 3 * time.Millisecond
		slowDelay   = 35 * time.Millisecond // >10x slower straggler
	)

	staticDur, _, _ := runSimulated4WorkerPool(t, SchedModeStatic, totalChunks, fastDelay, slowDelay)
	dynamicDur, fastCompleted, dynStats := runSimulated4WorkerPool(t, SchedModeDynamic, totalChunks, fastDelay, slowDelay)

	t.Logf("38 chunks (3x 3ms fast + 1x 35ms slow): static=%v, dynamic=%v, stolen=%d, fastShare=%d/%d",
		staticDur, dynamicDur, dynStats.StolenChunks, fastCompleted, totalChunks)

	if dynStats.CompletedChunks != totalChunks {
		t.Fatalf("CompletedChunks = %d, want %d", dynStats.CompletedChunks, totalChunks)
	}
	if dynStats.StolenChunks == 0 {
		t.Fatalf("expected dynamic scheduler to steal stuck chunks from 10x slow worker, got StolenChunks=0")
	}
	// Fast workers should carry >85% of effective chunk completions.
	if fastCompleted*100 < totalChunks*85 {
		t.Fatalf("expected fast workers to complete >=85%% of chunks, got %d/%d", fastCompleted, totalChunks)
	}
	if dynamicDur >= staticDur*80/100 {
		t.Fatalf("expected dynamic duration (%v) to improve clearly (>20%%) over static (%v)", dynamicDur, staticDur)
	}
}

func TestAdaptiveChunkSchedulerMidTransferSteal3xMedian(t *testing.T) {
	const chunkSize = int64(32)
	var stealEvents atomic.Int32
	probe := &callbackLockProbeInspector{}

	sched := newAdaptiveChunkScheduler(nil, chunkSize*20, chunkSize, probe, nil, AdaptiveSchedulerConfig{
		MidStealMultiplier:   3.0,
		MinMidStealThreshold: 4 * time.Millisecond,
	})
	probe.sched = sched

	// Worker 3 claims chunk 0 and stalls while plenty of Pending chunks remain.
	stuckTicket, ok := sched.Claim(3)
	if !ok || stuckTicket.Offset != 0 {
		t.Fatalf("expected worker 3 to claim offset 0, got %+v, ok=%v", stuckTicket, ok)
	}

	// Immediately trying to steal before 3 * median (3 * 5ms = 15ms) must return false.
	if _, stolen := sched.Steal(0); stolen {
		t.Fatalf("must not steal chunk before 3 * medianChunkDuration has elapsed")
	}

	// Wait past 3 * 5ms = 15ms while Pending still has 19 chunks.
	time.Sleep(18 * time.Millisecond)

	stolenTicket, stolen := sched.Steal(0)
	if !stolen {
		t.Fatalf("expected fast worker 0 to steal overdue InFlight chunk mid-transfer")
	}
	stealEvents.Add(1)
	if !stolenTicket.IsStolen || stolenTicket.Offset != stuckTicket.Offset {
		t.Fatalf("unexpected stolen ticket: %+v", stolenTicket)
	}

	// A third worker must not double-steal the same chunk (MaxConcurrentChunkOwners = 2).
	if _, doubleStolen := sched.Steal(1); doubleStolen {
		t.Fatalf("expected MaxConcurrentChunkOwners=2 to block a second stealer on offset 0")
	}
	if st := sched.Stats(); st.StolenChunks != 1 {
		t.Fatalf("StolenChunks = %d, want 1", st.StolenChunks)
	}
}

func TestAdaptiveChunkSchedulerDualCompleteIdempotent(t *testing.T) {
	const (
		chunkSize   = int64(32)
		totalChunks = 16
	)
	var doneCalls atomic.Int32
	sched := newAdaptiveChunkScheduler(nil, chunkSize*totalChunks, chunkSize, nil, func() {
		doneCalls.Add(1)
	}, AdaptiveSchedulerConfig{
		MinMidStealThreshold:     2 * time.Millisecond,
		MinEndgameStealThreshold: 1 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Do NOT cancel slow worker on completion: let both Primary and Stealer call Complete()
	// to verify firstCompletion idempotency and exact onDone accounting.
	var firstCompletions atomic.Int32
	var totalCompletions atomic.Int32

	var wg sync.WaitGroup
	for workerID := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			delay := 2 * time.Millisecond
			if id == 3 {
				delay = 18 * time.Millisecond
			}
			for {
				tk, ok := sched.Acquire(ctx, id)
				if !ok {
					return
				}
				time.Sleep(delay)
				totalCompletions.Add(1)
				if sched.Complete(id, tk, int(chunkSize), delay) {
					firstCompletions.Add(1)
				}
			}
		}(workerID)
	}
	wg.Wait()

	if firstCompletions.Load() != totalChunks {
		t.Fatalf("firstCompletions = %d, want exact %d (totalCompletions=%d)",
			firstCompletions.Load(), totalChunks, totalCompletions.Load())
	}
	if doneCalls.Load() != 1 {
		t.Fatalf("onDone called %d times, want 1", doneCalls.Load())
	}
}

func TestAdaptiveChunkSchedulerNoLivelockWhenOnlySlowWorkerRemains(t *testing.T) {
	const (
		chunkSize   = int64(32)
		totalChunks = 20
	)
	var doneCalls atomic.Int32
	sched := newAdaptiveChunkScheduler(nil, chunkSize*totalChunks, chunkSize, nil, func() {
		doneCalls.Add(1)
	}, AdaptiveSchedulerConfig{
		SlowMinCooldown:      1 * time.Millisecond,
		SlowMaxCooldown:      3 * time.Millisecond,
		TailProtectionFactor: 3,
		WorkerActiveWindow:   12 * time.Millisecond,
	})

	// Seed speed estimator: workers 0, 1 are Fast (1ms), worker 2 is Slow (10ms).
	for id := range 2 {
		tk, ok := sched.Claim(id)
		if !ok {
			t.Fatalf("initial fast claim failed")
		}
		sched.Complete(id, tk, int(chunkSize), time.Millisecond)
	}
	slowTk, ok := sched.Claim(2)
	if !ok {
		t.Fatalf("initial slow claim failed")
	}
	sched.Complete(2, slowTk, int(chunkSize), 10*time.Millisecond)

	// Now fast workers 0 and 1 disconnect completely; ONLY slow worker 2 remains!
	// Even at the tail of the file, once WorkerActiveWindow (12ms) expires, worker 2
	// must complete every remaining chunk without livelock.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for {
		tk, acquired := sched.Acquire(ctx, 2)
		if !acquired {
			break
		}
		sched.Complete(2, tk, int(chunkSize), 2*time.Millisecond)
	}

	st := sched.Stats()
	if st.CompletedChunks != totalChunks || doneCalls.Load() != 1 {
		t.Fatalf("expected surviving slow worker to finish all %d chunks, got stats=%+v doneCalls=%d",
			totalChunks, st, doneCalls.Load())
	}
}

func TestAdaptiveChunkSchedulerStealerReleaseAllowsReSteal(t *testing.T) {
	const chunkSize = int64(32)
	sched := newAdaptiveChunkScheduler(nil, chunkSize, chunkSize, nil, nil, AdaptiveSchedulerConfig{
		MinEndgameStealThreshold: time.Millisecond,
	})

	// Worker 0 claims the sole chunk and hangs.
	tk0, ok := sched.Claim(0)
	if !ok || tk0.Offset != 0 {
		t.Fatalf("expected worker 0 to claim chunk 0, got %+v ok=%v", tk0, ok)
	}
	time.Sleep(3 * time.Millisecond)

	// Worker 1 steals the chunk, then fails and calls Release.
	tk1, ok := sched.Steal(1)
	if !ok || !tk1.IsStolen {
		t.Fatalf("expected worker 1 to steal chunk")
	}
	sched.Release(1, tk1)

	// Worker 2 must now be able to steal the chunk again and complete it.
	tk2, ok := sched.Steal(2)
	if !ok || !tk2.IsStolen {
		t.Fatalf("expected worker 2 to re-steal chunk after worker 1 released it")
	}
	if !sched.Complete(2, tk2, int(chunkSize), time.Millisecond) {
		t.Fatalf("expected worker 2 completion to succeed")
	}
	if st := sched.Stats(); st.CompletedChunks != 1 {
		t.Fatalf("CompletedChunks = %d, want 1", st.CompletedChunks)
	}
}

func TestAdaptiveChunkSchedulerRaceStress(t *testing.T) {
	const (
		chunkSize   = int64(64)
		totalChunks = 96
	)
	sched := newAdaptiveChunkScheduler(nil, chunkSize*totalChunks, chunkSize, nil, nil, AdaptiveSchedulerConfig{
		SlowMinCooldown:          time.Millisecond,
		SlowMaxCooldown:          3 * time.Millisecond,
		MinMidStealThreshold:     2 * time.Millisecond,
		MinEndgameStealThreshold: time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for workerID := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			iter := 0
			for {
				tk, ok := sched.Acquire(ctx, id)
				if !ok {
					return
				}
				iter++
				if id%4 == 3 {
					time.Sleep(4 * time.Millisecond)
				} else {
					time.Sleep(400 * time.Microsecond)
				}
				// Inject periodic transient send errors on primary/stealer tickets.
				if iter%7 == 0 {
					sched.Release(id, tk)
					continue
				}
				sched.Complete(id, tk, int(chunkSize), 500*time.Microsecond)
			}
		}(workerID)
	}

	// Inject a concurrent selective repair request mid-flight.
	time.Sleep(4 * time.Millisecond)
	sched.ReenqueueRanges([]int64{chunkSize, 0, 4, chunkSize * 10, 3})

	wg.Wait()

	st := sched.Stats()
	if st.CompletedChunks != totalChunks || st.PendingChunks != 0 || st.InFlightChunks != 0 {
		t.Fatalf("unexpected final race-stress stats: %+v", st)
	}
}

func TestChunkSchedulerEnumsAndOverflowBounds(t *testing.T) {
	stateTests := []struct {
		state ChunkState
		want  string
	}{
		{ChunkStatePending, "Pending"},
		{ChunkStateInFlight, "InFlight"},
		{ChunkStateDone, "Done"},
		{ChunkState(99), "Unknown"},
	}
	for _, tc := range stateTests {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("ChunkState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}

	tierTests := []struct {
		tier WorkerTier
		want string
	}{
		{WorkerNormal, "Normal"},
		{WorkerFast, "Fast"},
		{WorkerSlow, "Slow"},
		{WorkerTier(99), "Unknown"},
	}
	for _, tc := range tierTests {
		if got := tc.tier.String(); got != tc.want {
			t.Fatalf("WorkerTier(%d).String() = %q, want %q", tc.tier, got, tc.want)
		}
	}

	// Empty 0-byte file must reject ReenqueueRanges safely without spinning.
	emptySched := newStaticChunkScheduler(nil, 0, 32, nil, nil)
	if _, stolen := emptySched.Steal(0); stolen {
		t.Fatalf("static Steal must always return false")
	}
	if n := emptySched.ReenqueueRanges([]int64{32, 0, math.MaxInt64}); n != 0 {
		t.Fatalf("expected 0 chunks enqueued on 0-byte file, got %d", n)
	}

	// Unsorted initial ranges + huge count overflow guard + high worker IDs (>=16).
	unsortedRanges := []int64{32, 64, 1, 0, 2}
	dynSched := newAdaptiveChunkScheduler(unsortedRanges, 128, 32, nil, nil, AdaptiveSchedulerConfig{
		SlowMinCooldown: 10 * time.Millisecond,
		SlowMaxCooldown: 2 * time.Millisecond, // exercises SlowMaxCooldown < SlowMinCooldown clamp
	})

	// Worker ID 20 exercises largeTiers snapshot map (>15).
	tk, ok := dynSched.Claim(20)
	if !ok {
		t.Fatalf("expected high workerID=20 to claim chunk")
	}
	dynSched.Complete(20, tk, 32, time.Millisecond)

	// Huge count must be clamped strictly by fileSize (128 bytes = 4 chunks max, 0..96) without int64 overflow!
	enqueued := dynSched.ReenqueueRanges([]int64{
		32,
		64, math.MaxInt64, // clamped to offsets 64, 96
		math.MaxInt64 - 8, 10000000, // >= fileSize -> safely ignored
	})
	if enqueued != 2 {
		t.Fatalf("expected clamped ReenqueueRanges to enqueue 2 chunks (64, 96), got %d", enqueued)
	}
}
