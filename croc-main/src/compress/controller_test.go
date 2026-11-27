package compress

import (
	"compress/flate"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestController_InitialState(t *testing.T) {
	ctrl := NewAdaptiveController()
	assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
}

func TestController_SlowNetwork(t *testing.T) {
	t.Run("50ms_send_delay", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		for range 20 {
			ctrl.RecordChunk(time.Millisecond, 50*time.Millisecond)
		}
		assert.Equal(t, flate.DefaultCompression, ctrl.RecommendLevel())
	})

	// 32 KiB over a 1 Mbps link takes ~262 ms to transmit; network dominates so
	// controller should step up to DefaultCompression to shrink wire bytes.
	t.Run("simulated_1Mbps_link", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		for range 20 {
			ctrl.RecordChunk(2*time.Millisecond, 262*time.Millisecond)
		}
		assert.Equal(t, flate.DefaultCompression, ctrl.RecommendLevel())
	})
}

func TestController_FastNetwork(t *testing.T) {
	t.Run("1ms_send_delay", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		for range 20 {
			ctrl.RecordChunk(5*time.Millisecond, time.Millisecond)
		}
		assert.Equal(t, flate.HuffmanOnly, ctrl.RecommendLevel())
	})

	// 32 KiB over a 1 Gbps link takes ~262 us to transmit; compression CPU
	// dominates so controller should step down all the way to HuffmanOnly.
	t.Run("simulated_1Gbps_link_steps_one_tier_at_a_time", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		// Drive up to DefaultCompression first.
		for range 10 {
			ctrl.RecordChunk(time.Millisecond, 262*time.Millisecond)
		}
		assert.Equal(t, flate.DefaultCompression, ctrl.RecommendLevel())

		// Feed sustained 1 Gbps fast-link timings: expect step-by-step downgrade
		// DefaultCompression -> BestSpeed -> HuffmanOnly (not jumping directly).
		sawBestSpeed := false
		for range 30 {
			ctrl.RecordChunk(3*time.Millisecond, 262*time.Microsecond)
			if ctrl.RecommendLevel() == flate.BestSpeed {
				sawBestSpeed = true
			}
		}
		assert.True(t, sawBestSpeed, "controller must step through BestSpeed before HuffmanOnly")
		assert.Equal(t, flate.HuffmanOnly, ctrl.RecommendLevel())
	})
}

func TestController_Stable(t *testing.T) {
	ctrl := NewAdaptiveController()
	for range 30 {
		// cpuRatio ~ 2 / (2 + 5) = 0.286, squarely inside dead zone [0.15, 0.45].
		ctrl.RecordChunk(2*time.Millisecond, 5*time.Millisecond)
		assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
	}
}

func TestController_Hysteresis(t *testing.T) {
	ctrl := NewAdaptiveController()

	// 4 consecutive CPU-bottleneck chunks: below hystThreshold (5), must not switch.
	for range 4 {
		ctrl.RecordChunk(5*time.Millisecond, time.Millisecond)
	}
	assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())

	// 1 stabilizing chunk brings EWMA ratio into [0.15, 0.45] (~0.397), resetting counter.
	ctrl.RecordChunk(0, 12*time.Millisecond)
	assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())

	// Next 4 CPU-bottleneck chunks must still keep BestSpeed because counter was reset.
	for range 4 {
		ctrl.RecordChunk(5*time.Millisecond, time.Millisecond)
		assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
	}

	// 5th consecutive CPU-bottleneck chunk after reset triggers the downgrade.
	ctrl.RecordChunk(5*time.Millisecond, time.Millisecond)
	assert.Equal(t, flate.HuffmanOnly, ctrl.RecommendLevel())
}

func TestController_Oscillation(t *testing.T) {
	t.Run("alternating_single_chunks", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		for i := range 40 {
			if i%2 == 0 {
				ctrl.RecordChunk(10*time.Millisecond, time.Millisecond)
			} else {
				ctrl.RecordChunk(time.Millisecond, 10*time.Millisecond)
			}
			assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
		}
	})

	t.Run("alternating_short_bursts", func(t *testing.T) {
		ctrl := NewAdaptiveController()
		for range 8 {
			for range 3 {
				ctrl.RecordChunk(10*time.Millisecond, time.Millisecond)
				assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
			}
			for range 3 {
				ctrl.RecordChunk(time.Millisecond, 12*time.Millisecond)
				assert.Equal(t, flate.BestSpeed, ctrl.RecommendLevel())
			}
		}
	})
}

func TestController_ConcurrentAccess(t *testing.T) {
	ctrl := NewAdaptiveController()
	validLevels := []int{flate.HuffmanOnly, flate.BestSpeed, flate.DefaultCompression}

	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 200 {
				if (id+i)%2 == 0 {
					ctrl.RecordChunk(4*time.Millisecond, time.Millisecond)
				} else {
					ctrl.RecordChunk(time.Millisecond, 30*time.Millisecond)
				}
				assert.Contains(t, validLevels, ctrl.RecommendLevel())
			}
		}(g)
	}
	wg.Wait()
}

var controllerBenchSink int

func BenchmarkControllerRecordChunk(b *testing.B) {
	ctrl := NewAdaptiveController()
	b.ReportAllocs()
	for range b.N {
		ctrl.RecordChunk(2*time.Millisecond, 5*time.Millisecond)
		controllerBenchSink = ctrl.RecommendLevel()
	}
}
