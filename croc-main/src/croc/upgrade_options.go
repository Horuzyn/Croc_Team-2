package croc

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/telemetry"
)

// parseWorkerThrottle parses a --debug-throttle-worker value "<id>:<rate>",
// for example "0:512k". The rate uses the --throttleUpload syntax and is
// returned in bytes per second.
func parseWorkerThrottle(value string) (workerID int, bytesPerSecond int64, err error) {
	id, rate, found := strings.Cut(value, ":")
	// parseUploadLimit panics on an empty string.
	if found && rate != "" {
		workerID, err = strconv.Atoi(id)
		if err == nil && workerID >= 0 {
			bytesPerSecond, err = parseUploadLimit(rate)
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
	switch c.Options.SchedMode {
	case "", "static", "dynamic":
	default:
		return fmt.Errorf("invalid scheduler %q (choose static or dynamic)", c.Options.SchedMode)
	}
	if c.Options.DebugThrottleWorker != "" {
		if _, _, err = parseWorkerThrottle(c.Options.DebugThrottleWorker); err != nil {
			return err
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
