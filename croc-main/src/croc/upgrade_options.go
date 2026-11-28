package croc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/telemetry"
	"golang.org/x/time/rate"
)

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
	return c.Options.SchedMode != SchedModeStatic && c.peerDynamicChunkSched
}

// unmarkReceivedChunk clears the idempotent receive bit for offset so selective
// Merkle chunk repair rounds (MRK-04/MRK-05) can re-count repaired chunks cleanly.
func (c *Client) unmarkReceivedChunk(offset int64) {
	c.receiveMutex.Lock()
	c.receivedChunks.Clear(offset)
	c.receiveMutex.Unlock()
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
	case <-c.stop.ctx.Done():
		res.Cancel()
		return false
	case <-attemptDone:
		res.Cancel()
		return false
	}
}
