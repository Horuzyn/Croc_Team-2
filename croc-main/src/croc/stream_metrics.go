package croc

import (
	"context"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/telemetry"
)

const (
	// DefaultStreamTelemetryInterval is the 200 ms JSONL reporting cadence specified by SCH-02.
	DefaultStreamTelemetryInterval = 200 * time.Millisecond
)

// StreamTrackerConfig configures EWMA smoothing, tier thresholds, and JSONL
// emission cadence for StreamTracker.
type StreamTrackerConfig struct {
	EWMAAlpha          float64
	FastRatioThreshold float64
	SlowRatioThreshold float64
	MinStallObserve    time.Duration
	ColdStallObserve   time.Duration
	TelemetryInterval  time.Duration
}

// DefaultStreamTrackerConfig returns production defaults for SCH-02.
func DefaultStreamTrackerConfig() StreamTrackerConfig {
	return StreamTrackerConfig{
		EWMAAlpha:          0.35,
		FastRatioThreshold: 0.85,
		SlowRatioThreshold: 0.50,
		MinStallObserve:    10 * time.Millisecond,
		ColdStallObserve:   50 * time.Millisecond,
		TelemetryInterval:  DefaultStreamTelemetryInterval,
	}
}

func normalizeStreamTrackerConfig(cfg StreamTrackerConfig) StreamTrackerConfig {
	def := DefaultStreamTrackerConfig()
	if cfg.EWMAAlpha <= 0 || cfg.EWMAAlpha > 1 {
		cfg.EWMAAlpha = def.EWMAAlpha
	}
	if cfg.FastRatioThreshold <= 0 || cfg.FastRatioThreshold > 1 {
		cfg.FastRatioThreshold = def.FastRatioThreshold
	}
	if cfg.SlowRatioThreshold <= 0 || cfg.SlowRatioThreshold >= cfg.FastRatioThreshold {
		cfg.SlowRatioThreshold = def.SlowRatioThreshold
	}
	if cfg.MinStallObserve <= 0 {
		cfg.MinStallObserve = def.MinStallObserve
	}
	if cfg.ColdStallObserve <= 0 {
		cfg.ColdStallObserve = def.ColdStallObserve
	}
	if cfg.TelemetryInterval <= 0 {
		cfg.TelemetryInterval = def.TelemetryInterval
	}
	return cfg
}

type workerStreamMetrics struct {
	workerID      int
	localPort     int
	ewmaBps       float64
	peakBps       float64
	ewmaDur       time.Duration
	samples       int
	inflight      int
	inflightBytes int
	inflightStart time.Time
	stolenChunks  uint64
}

// StreamTracker tracks per-TCP-worker EWMA throughput and chunk send latency,
// classifies streams into Fast/Normal/Slow relative to pool speed, and emits
// periodic stream_stat JSONL records via telemetry.Sink (SCH-02).
//
// Lock ordering: StreamTracker is a leaf component and never calls into
// ChunkScheduler; all telemetry.Sink writes happen strictly outside mu.
type StreamTracker struct {
	cfg  StreamTrackerConfig
	sink telemetry.Sink

	mu      sync.RWMutex
	workers map[int]*workerStreamMetrics

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewStreamTracker creates a StreamTracker writing periodic stream_stat events
// to sink (pass telemetry.Nop or nil when --metrics-out is disabled).
func NewStreamTracker(sink telemetry.Sink, cfg ...StreamTrackerConfig) *StreamTracker {
	if sink == nil {
		sink = telemetry.Nop
	}
	var resolved StreamTrackerConfig
	if len(cfg) > 0 {
		resolved = normalizeStreamTrackerConfig(cfg[0])
	} else {
		resolved = DefaultStreamTrackerConfig()
	}
	return &StreamTracker{
		cfg:     resolved,
		sink:    sink,
		workers: make(map[int]*workerStreamMetrics),
	}
}

// RegisterWorker records the local TCP port associated with workerID.
func (t *StreamTracker) RegisterWorker(workerID int, localPort int) {
	if workerID < 0 {
		return
	}
	t.mu.Lock()
	w := t.getOrCreateLocked(workerID)
	if localPort > 0 {
		w.localPort = localPort
	}
	t.mu.Unlock()
}

// RegisterWorkerConn extracts the local TCP port from dataConn and associates
// it with workerID.
func (t *StreamTracker) RegisterWorkerConn(workerID int, dataConn *comm.Comm) {
	t.RegisterWorker(workerID, extractCommLocalPort(dataConn))
}

func extractCommLocalPort(dataConn *comm.Comm) int {
	if dataConn == nil {
		return 0
	}
	conn := dataConn.Connection()
	if conn == nil {
		return 0
	}
	addr := conn.LocalAddr()
	if addr == nil {
		return 0
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return tcpAddr.Port
	}
	_, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port
}

func (t *StreamTracker) getOrCreateLocked(workerID int) *workerStreamMetrics {
	w := t.workers[workerID]
	if w == nil {
		w = &workerStreamMetrics{workerID: workerID}
		t.workers[workerID] = w
	}
	return w
}

// MarkSendStart marks a chunk transmission as in-flight on workerID so live
// socket stalls are detected before dataConn.Send returns and reflected in
// telemetry.StreamStat.Inflight. Pair with MarkSendFinish via defer.
func (t *StreamTracker) MarkSendStart(workerID int, pendingBytes int) {
	if workerID < 0 {
		return
	}
	if pendingBytes <= 0 {
		pendingBytes = models.TCP_BUFFER_SIZE / 2
	}
	now := time.Now()
	t.mu.Lock()
	w := t.getOrCreateLocked(workerID)
	if w.inflight == 0 {
		w.inflightStart = now
		w.inflightBytes = pendingBytes
	} else {
		w.inflightBytes += pendingBytes
	}
	w.inflight++
	t.mu.Unlock()
}

// MarkSendFinish decrements the active in-flight send count for workerID.
func (t *StreamTracker) MarkSendFinish(workerID int) {
	if workerID < 0 {
		return
	}
	t.mu.Lock()
	if w := t.workers[workerID]; w != nil && w.inflight > 0 {
		w.inflight--
		if w.inflight == 0 {
			w.inflightStart = time.Time{}
			w.inflightBytes = 0
		}
	}
	t.mu.Unlock()
}

// RecordSendSample updates workerID's EWMA throughput (bytes/sec) and EWMA
// chunk send latency after completing a socket write.
func (t *StreamTracker) RecordSendSample(workerID int, bytes int, sendDur time.Duration) {
	if workerID < 0 {
		return
	}
	if bytes <= 0 && sendDur <= 0 {
		return
	}
	if bytes <= 0 {
		bytes = 1
	}
	if sendDur <= 0 {
		sendDur = time.Microsecond
	}
	instBps := float64(bytes) / sendDur.Seconds()

	t.mu.Lock()
	w := t.getOrCreateLocked(workerID)
	if w.samples == 0 {
		w.ewmaBps = instBps
		w.peakBps = instBps
		w.ewmaDur = sendDur
		w.samples = 1
	} else {
		alpha := t.cfg.EWMAAlpha
		w.ewmaBps = alpha*instBps + (1.0-alpha)*w.ewmaBps
		w.ewmaDur = time.Duration(alpha*float64(sendDur) + (1.0-alpha)*float64(w.ewmaDur))
		if w.ewmaBps > w.peakBps {
			w.peakBps = w.ewmaBps
		}
		w.samples++
	}
	t.mu.Unlock()
}

// RecordSteal increments the stolen chunk counter for thiefWorkerID.
func (t *StreamTracker) RecordSteal(thiefWorkerID int) {
	if thiefWorkerID < 0 {
		return
	}
	t.mu.Lock()
	w := t.getOrCreateLocked(thiefWorkerID)
	w.stolenChunks++
	t.mu.Unlock()
}

// effectiveBpsLocked computes worker w's live throughput, capping historical
// EWMA when an in-flight socket write is actively stalling.
func (t *StreamTracker) effectiveBpsLocked(w *workerStreamMetrics, now time.Time) (bps float64, ok bool) {
	if w == nil {
		return 0, false
	}
	bps = w.ewmaBps
	hasEstimate := w.samples > 0

	if w.inflight > 0 && !w.inflightStart.IsZero() {
		elapsed := now.Sub(w.inflightStart)
		bytes := w.inflightBytes
		if bytes <= 0 {
			bytes = models.TCP_BUFFER_SIZE / 2
		}
		if w.samples > 0 {
			stallFloor := max(2*w.ewmaDur, t.cfg.MinStallObserve)
			if elapsed > stallFloor {
				liveCap := float64(bytes) / elapsed.Seconds()
				if liveCap < bps {
					bps = liveCap
				}
			}
		} else if elapsed >= t.cfg.ColdStallObserve {
			bps = float64(bytes) / elapsed.Seconds()
			hasEstimate = true
		}
	}
	return bps, hasEstimate
}

func (t *StreamTracker) effectiveDurLocked(w *workerStreamMetrics, now time.Time) (time.Duration, bool) {
	if w == nil {
		return 0, false
	}
	dur := w.ewmaDur
	hasEstimate := w.samples > 0

	if w.inflight > 0 && !w.inflightStart.IsZero() {
		elapsed := now.Sub(w.inflightStart)
		if w.samples > 0 {
			stallFloor := max(2*w.ewmaDur, t.cfg.MinStallObserve)
			if elapsed > stallFloor && elapsed > dur {
				dur = elapsed
			}
		} else if elapsed >= t.cfg.ColdStallObserve {
			dur = elapsed
			hasEstimate = true
		}
	}
	return dur, hasEstimate
}

// WorkerThroughputBps returns the current effective throughput (bytes/sec) for workerID.
func (t *StreamTracker) WorkerThroughputBps(workerID int) float64 {
	now := time.Now()
	t.mu.RLock()
	bps, _ := t.effectiveBpsLocked(t.workers[workerID], now)
	t.mu.RUnlock()
	return bps
}

// MedianThroughputBps returns the pool's reference median throughput (bytes/sec).
func (t *StreamTracker) MedianThroughputBps() float64 {
	now := time.Now()
	t.mu.RLock()
	var buf [16]float64
	rates := buf[:0]
	for _, w := range t.workers {
		if bps, ok := t.effectiveBpsLocked(w, now); ok && bps > 0 {
			rates = append(rates, bps)
		}
	}
	t.mu.RUnlock()
	return upperMedianFloat64(rates)
}

// MedianChunkDuration implements StreamSpeedInspector.
func (t *StreamTracker) MedianChunkDuration() time.Duration {
	now := time.Now()
	t.mu.RLock()
	var buf [16]time.Duration
	durs := buf[:0]
	for _, w := range t.workers {
		if d, ok := t.effectiveDurLocked(w, now); ok && d > 0 {
			durs = append(durs, d)
		}
	}
	t.mu.RUnlock()
	return medianDuration(durs)
}

// ClassifyWorker implements StreamSpeedInspector, classifying workerID into
// Fast, Normal, or Slow relative to the pool's reference median speed and its
// own historical baseline.
func (t *StreamTracker) ClassifyWorker(workerID int) WorkerTier {
	now := time.Now()
	t.mu.RLock()
	w := t.workers[workerID]
	workerBps, hasWorker := t.effectiveBpsLocked(w, now)
	if !hasWorker || workerBps <= 0 {
		t.mu.RUnlock()
		return WorkerNormal
	}
	peakBps := w.peakBps
	samples := w.samples

	var buf [16]float64
	rates := buf[:0]
	for _, peer := range t.workers {
		if bps, ok := t.effectiveBpsLocked(peer, now); ok && bps > 0 {
			rates = append(rates, bps)
		}
	}
	t.mu.RUnlock()

	// Check self-degradation (>50% drop from own peak after initial warm-up).
	if samples >= 2 && peakBps > 0 && workerBps < t.cfg.SlowRatioThreshold*peakBps {
		return WorkerSlow
	}
	if len(rates) < 2 {
		return WorkerNormal
	}

	refBps := upperMedianFloat64(rates)
	if refBps <= 0 {
		return WorkerNormal
	}
	if workerBps < t.cfg.SlowRatioThreshold*refBps {
		return WorkerSlow
	}
	if workerBps >= t.cfg.FastRatioThreshold*refBps {
		return WorkerFast
	}
	return WorkerNormal
}

func upperMedianFloat64(vals []float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	slices.Sort(vals)
	return vals[n/2]
}

// Start launches the background 200 ms telemetry emitter when a non-nop Sink
// is configured. Safe to call multiple times.
func (t *StreamTracker) Start(ctx context.Context) {
	if t.sink == nil || t.sink == telemetry.Nop {
		return
	}
	t.lifecycleMu.Lock()
	if t.started || t.stopped {
		t.lifecycleMu.Unlock()
		return
	}
	t.started = true
	t.stopCh = make(chan struct{})
	stopCh := t.stopCh
	t.wg.Add(1)
	t.lifecycleMu.Unlock()

	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(t.cfg.TelemetryInterval)
		defer ticker.Stop()
		for {
			if ctx == nil {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					t.EmitSnapshotNow()
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case <-stopCh:
					return
				case <-ticker.C:
					t.EmitSnapshotNow()
				}
			}
		}
	}()
}

// Stop halts the periodic telemetry goroutine and flushes one final stream_stat
// snapshot per tracked worker so short transfers (<200 ms) still appear in JSONL.
func (t *StreamTracker) Stop() {
	t.lifecycleMu.Lock()
	if t.stopped {
		t.lifecycleMu.Unlock()
		return
	}
	t.stopped = true
	wasStarted := t.started
	if wasStarted && t.stopCh != nil {
		close(t.stopCh)
	}
	t.lifecycleMu.Unlock()

	if wasStarted {
		t.wg.Wait()
	}
	t.EmitSnapshotNow()
}

// EmitSnapshotNow snapshots all tracked workers and writes telemetry.StreamStat
// events to Sink outside of internal locks.
func (t *StreamTracker) EmitSnapshotNow() {
	if t.sink == nil || t.sink == telemetry.Nop {
		return
	}
	now := time.Now()

	t.mu.RLock()
	if len(t.workers) == 0 {
		t.mu.RUnlock()
		return
	}
	stats := make([]telemetry.StreamStat, 0, len(t.workers))
	for _, w := range t.workers {
		bps, _ := t.effectiveBpsLocked(w, now)
		stats = append(stats, telemetry.StreamStat{
			WorkerID:      w.workerID,
			LocalPort:     w.localPort,
			ThroughputBps: bps,
			Inflight:      w.inflight,
			StolenChunks:  w.stolenChunks,
		})
	}
	t.mu.RUnlock()

	slices.SortFunc(stats, func(a, b telemetry.StreamStat) int {
		return a.WorkerID - b.WorkerID
	})
	for _, st := range stats {
		t.sink.Emit(st)
	}
}
