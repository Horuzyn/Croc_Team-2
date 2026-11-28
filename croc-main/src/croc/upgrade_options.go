package croc

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/telemetry"
	"golang.org/x/time/rate"
)

const multiFilePacketDrainTimeout = 5 * time.Second

// parseWorkerThrottle parses a --debug-throttle-worker value "<id>:<rate>",
// for example "0:512k". The rate uses the --throttleUpload syntax and is
// returned in bytes per second.
func parseWorkerThrottle(value string) (workerID int, bytesPerSecond int64, err error) {
	id, rateStr, found := strings.Cut(value, ":")
	// parseUploadLimit panics on an empty string.
	if found && rateStr != "" {
		workerID, err = strconv.Atoi(id)
		if err == nil && workerID >= 0 {
			bytesPerSecond, err = parseUploadLimit(rateStr)
			if err == nil {
				return workerID, bytesPerSecond, nil
			}
		}
	}
	return 0, 0, fmt.Errorf("invalid worker throttle %q (want <id>:<rate>, e.g. 0:512k)", value)
}

// initUpgrades validates the upgrade options and opens the metrics sink.
// Call it last in newClient so no earlier error leaks the open file.
func (c *Client) initUpgrades() (err error) {
	c.debugThrottleWorkerID = -1
	switch c.Options.SchedMode {
	case "", SchedModeStatic, SchedModeDynamic:
	default:
		return fmt.Errorf("invalid scheduler %q (choose static or dynamic)", c.Options.SchedMode)
	}
	if c.Options.DebugThrottleWorker != "" {
		workerID, bps, parseErr := parseWorkerThrottle(c.Options.DebugThrottleWorker)
		if parseErr != nil {
			return parseErr
		}
		if c.Options.IsSender {
			c.debugThrottleWorkerID = workerID
			burst := max(models.TCP_BUFFER_SIZE/2, 1)
			c.debugWorkerLimiter = rate.NewLimiter(rate.Limit(bps), burst)
		}
	}
	c.metrics, err = telemetry.Open(c.Options.MetricsOut)
	return err
}

// closeMetrics reports, but never returns, a telemetry write failure.
func (c *Client) closeMetrics() {
	if c.metrics == nil {
		return
	}
	if err := c.metrics.Close(); err != nil {
		log.Warnf("metrics file is incomplete: %v", err)
	}
}

// effectiveSchedMode resolves whether work-stealing dynamic scheduling is active
// for the current session. If the peer lacks FeatureDynamicChunkSchedV1 or the
// user explicitly requested --sched static, static scheduling is used safely.
func (c *Client) effectiveSchedMode() string {
	if c.Options.SchedMode == SchedModeStatic {
		return SchedModeStatic
	}
	if !c.peerDynamicChunkSched {
		return SchedModeStatic
	}
	return SchedModeDynamic
}

func (c *Client) dynamicSchedReceiveActive() bool {
	return !c.Options.IsSender && c.effectiveSchedMode() == SchedModeDynamic
}

// unmarkReceivedChunk clears both in-flight and completed idempotent receive
// bits for offset so selective Merkle chunk repair (MRK-04/MRK-05) can re-write
// and re-count repaired chunks cleanly. Callers must invoke this between receive
// passes before requesting repair chunks.
func (c *Client) unmarkReceivedChunk(offset int64) {
	c.receiveMutex.Lock()
	c.inflightReceiveChunks.Clear(offset)
	c.receivedChunks.Clear(offset)
	c.receiveMutex.Unlock()
}

// unmarkReceivedChunkRanges clears both receive bitsets for every chunk offset
// covered by ranges (standard [chunkSize, start1, count1, ...] format) while
// preserving duplicate protection for all healthy chunks from previous passes.
// Callers must invoke this between receive passes before requesting repair chunks.
func (c *Client) unmarkReceivedChunkRanges(ranges []int64, fileSize int64) int {
	if fileSize <= 0 || len(ranges) < 3 || ranges[0] <= 0 {
		return 0
	}
	chunkSize := ranges[0]
	c.receiveMutex.Lock()
	defer c.receiveMutex.Unlock()

	if c.receivedChunks.chunkSize > 0 && chunkSize != c.receivedChunks.chunkSize {
		log.Debugf("unmarkReceivedChunkRanges chunk size mismatch: got %d, want %d", chunkSize, c.receivedChunks.chunkSize)
		return 0
	}

	cleared := 0
	for i := 1; i+1 < len(ranges); i += 2 {
		start, count := ranges[i], ranges[i+1]
		if start < 0 || start >= fileSize || count <= 0 {
			continue
		}
		maxSteps := (fileSize-1-start)/chunkSize + 1
		if count > maxSteps {
			count = maxSteps
		}
		for step := range count {
			off := start + step*chunkSize
			wasMarked := c.inflightReceiveChunks.Test(off) || c.receivedChunks.Test(off)
			c.inflightReceiveChunks.Clear(off)
			c.receivedChunks.Clear(off)
			if wasMarked {
				cleared++
			}
		}
	}
	return cleared
}

func encodeSentPacketsPayload(sentPackets int64, dynamicActive bool) []byte {
	if !dynamicActive || sentPackets <= 0 {
		return nil
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(sentPackets))
	return buf[:]
}

func decodeSentPacketsPayload(payload []byte) (int64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) != 8 {
		log.Debugf("ignoring malformed close-recipient packet count length %d", len(payload))
		return 0, false
	}
	v := binary.LittleEndian.Uint64(payload)
	if v == 0 || v > math.MaxInt64 {
		log.Debugf("ignoring out-of-range close-recipient packet count %d", v)
		return 0, false
	}
	return int64(v), true
}

func (c *Client) noteReceivedDataPacketLocked() {
	c.recvDataPackets++
	if c.recvDrainNotify != nil {
		select {
		case c.recvDrainNotify <- struct{}{}:
		default:
		}
	}
}

// waitForReceivePacketDrain ensures that before a multi-file session opens the
// next file on shared TCP data connections, any delayed duplicate packet sent
// during the previous file unit has been read and discarded. If the timeout
// expires with packets still unconsumed, it returns transferDisconnectError to
// prevent late packets from corrupting subsequent files.
func (c *Client) waitForReceivePacketDrain(attempt *transferAttemptState, targetPackets int64) error {
	return c.waitForReceivePacketDrainWithTimeout(attempt, targetPackets, multiFilePacketDrainTimeout)
}

func (c *Client) waitForReceivePacketDrainWithTimeout(attempt *transferAttemptState, targetPackets int64, timeout time.Duration) error {
	if !c.dynamicSchedReceiveActive() || targetPackets <= 0 {
		return nil
	}
	if c.FilesToTransferCurrentNum+1 >= len(c.FilesToTransfer) {
		return nil
	}
	if timeout <= 0 {
		timeout = multiFilePacketDrainTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	clientDone := c.clientContext().Done()
	var attemptDone <-chan struct{}
	if attempt != nil {
		attemptDone = attempt.contextDone()
	}
	for {
		c.receiveMutex.Lock()
		recvPackets := c.recvDataPackets
		notifyCh := c.recvDrainNotify
		c.receiveMutex.Unlock()
		if recvPackets >= targetPackets {
			return nil
		}
		if notifyCh == nil {
			return transferDisconnectError{err: fmt.Errorf("missing drain notifier before next file (%d/%d packets)", recvPackets, targetPackets)}
		}
		select {
		case <-notifyCh:
		case <-clientDone:
			return c.ctxErr()
		case <-attemptDone:
			return attempt.ctx.Err()
		case <-timer.C:
			c.receiveMutex.Lock()
			finalRecv := c.recvDataPackets
			c.receiveMutex.Unlock()
			if finalRecv >= targetPackets {
				return nil
			}
			return transferDisconnectError{err: fmt.Errorf("timed out draining delayed packets before next file (%d/%d)", finalRecv, targetPackets)}
		}
	}
}

// SetNetSendObserver registers a callback notified with each chunk's socket
// write duration (netSendDuration) for adaptive compression control (CMP-03/04).
func (c *Client) SetNetSendObserver(fn func(netSendDur time.Duration)) {
	c.netSendObserverMu.Lock()
	c.netSendObserver = fn
	c.netSendObserverMu.Unlock()
}

// LastNetSendDuration returns the most recently observed socket write duration.
func (c *Client) LastNetSendDuration() time.Duration {
	return time.Duration(c.lastNetSendNs.Load())
}

func (c *Client) recordNetSendDuration(d time.Duration) {
	c.lastNetSendNs.Store(int64(d))
	c.netSendObserverMu.RLock()
	fn := c.netSendObserver
	c.netSendObserverMu.RUnlock()
	if fn != nil {
		fn(d)
	}
}

// waitDebugWorkerThrottle applies --debug-throttle-worker rate limiting to the
// targeted workerID, aborting promptly if unitCtx, stop, or attempt ends.
func (c *Client) waitDebugWorkerThrottle(
	unitCtx context.Context,
	attempt *transferAttemptState,
	workerID int,
	n int,
) bool {
	if c.debugWorkerLimiter == nil || workerID != c.debugThrottleWorkerID || n <= 0 {
		return true
	}
	res := c.debugWorkerLimiter.ReserveN(time.Now(), n)
	delay := res.Delay()
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	var unitDone <-chan struct{}
	if unitCtx != nil {
		unitDone = unitCtx.Done()
	}
	clientDone := c.clientContext().Done()
	var attemptDone <-chan struct{}
	if attempt != nil {
		attemptDone = attempt.contextDone()
	}

	select {
	case <-timer.C:
		return true
	case <-unitDone:
		res.Cancel()
		return false
	case <-clientDone:
		res.Cancel()
		return false
	case <-attemptDone:
		res.Cancel()
		return false
	}
}
