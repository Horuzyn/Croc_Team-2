package compress

import (
	"compress/flate"
	"sync"
	"time"
)

const (
	ewmaAlpha          = 0.3
	cpuBottleneckRatio = 0.45
	netBottleneckRatio = 0.15
	hystThreshold      = 5
	initialEWMANs      = float64(time.Millisecond)
)

// AdaptiveController tracks per-chunk compression and network send timings
// using EWMA smoothing and hysteresis to recommend the optimal flate level
// for upcoming chunks.
type AdaptiveController struct {
	mu           sync.Mutex
	ewmaCompress float64
	ewmaNet      float64
	level        int
	hystCount    int
	hystDir      int // -1 = trending lower, +1 = trending higher, 0 = stable
}

// NewAdaptiveController returns a controller starting at flate.BestSpeed with
// neutral 1ms EWMA priors so initial chunks do not trigger spurious shifts.
func NewAdaptiveController() *AdaptiveController {
	return &AdaptiveController{
		ewmaCompress: initialEWMANs,
		ewmaNet:      initialEWMANs,
		level:        flate.BestSpeed,
	}
}

// RecordChunk updates the EWMA timing estimates from the latest chunk and
// steps the recommended flate level by at most one tier after hystThreshold
// consecutive samples indicate the same bottleneck direction.
func (c *AdaptiveController) RecordChunk(compressDur, netSendDur time.Duration) {
	cNs := float64(max(compressDur, 0))
	nNs := float64(max(netSendDur, 0))

	c.mu.Lock()
	defer c.mu.Unlock()

	c.ewmaCompress = ewmaAlpha*cNs + (1-ewmaAlpha)*c.ewmaCompress
	c.ewmaNet = ewmaAlpha*nNs + (1-ewmaAlpha)*c.ewmaNet

	total := c.ewmaCompress + c.ewmaNet
	if total <= 0 {
		c.hystDir = 0
		c.hystCount = 0
		return
	}

	cpuRatio := c.ewmaCompress / total
	dir := 0
	switch {
	case cpuRatio > cpuBottleneckRatio:
		dir = -1
	case cpuRatio < netBottleneckRatio:
		dir = +1
	}

	if dir == 0 {
		c.hystDir = 0
		c.hystCount = 0
		return
	}

	nextLevel := stepLevel(c.level, dir)
	if nextLevel == c.level {
		c.hystDir = 0
		c.hystCount = 0
		return
	}

	if dir != c.hystDir {
		c.hystDir = dir
		c.hystCount = 1
		return
	}

	c.hystCount++
	if c.hystCount >= hystThreshold {
		c.level = nextLevel
		c.hystDir = 0
		c.hystCount = 0
	}
}

// RecommendLevel returns the current recommended flate compression level.
// Safe for concurrent use.
func (c *AdaptiveController) RecommendLevel() int {
	c.mu.Lock()
	lvl := c.level
	c.mu.Unlock()
	return lvl
}

func stepLevel(level, dir int) int {
	if dir < 0 {
		switch level {
		case flate.DefaultCompression:
			return flate.BestSpeed
		case flate.BestSpeed:
			return flate.HuffmanOnly
		default:
			return flate.HuffmanOnly
		}
	}
	if dir > 0 {
		switch level {
		case flate.HuffmanOnly:
			return flate.BestSpeed
		case flate.BestSpeed:
			return flate.DefaultCompression
		default:
			return flate.DefaultCompression
		}
	}
	return level
}
