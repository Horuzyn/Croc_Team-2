package compress

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"

	log "github.com/schollz/croc/v11/src/logger"
)

// LevelPool manages separate sync.Pools of flate.Writers for three
// compression levels, allowing adaptive chunk-level compression without
// allocating a new writer per chunk.
type LevelPool struct {
	pools [3]sync.Pool
}

var supportedLevels = [3]int{
	flate.HuffmanOnly,        // index 0 (-2)
	flate.BestSpeed,          // index 1 (1)
	flate.DefaultCompression, // index 2 (-1, equivalent to level 6)
}

func levelIndex(level int) int {
	switch level {
	case flate.BestSpeed:
		return 1
	case flate.DefaultCompression:
		return 2
	default:
		return 0
	}
}

// NewLevelPool creates a pool pre-configured for HuffmanOnly, BestSpeed,
// and DefaultCompression.
func NewLevelPool() *LevelPool {
	lp := &LevelPool{}
	for i, level := range supportedLevels {
		lvl := level
		lp.pools[i] = sync.Pool{
			New: func() any {
				w, err := flate.NewWriter(io.Discard, lvl)
				if err != nil {
					panic(err)
				}
				return w
			},
		}
	}
	return lp
}

// CompressTo compresses src at the given level, reusing dst's backing array
// when capacity suffices. Supported levels are flate.HuffmanOnly,
// flate.BestSpeed, and flate.DefaultCompression (others fall back to HuffmanOnly).
func (lp *LevelPool) CompressTo(dst, src []byte, level int) []byte {
	pool := &lp.pools[levelIndex(level)]
	w := pool.Get().(*flate.Writer)
	buf := bytes.NewBuffer(dst[:0])
	if cap(dst) < len(src) {
		buf.Grow(len(src) - cap(dst))
	}
	w.Reset(buf)
	if _, err := w.Write(src); err != nil {
		log.Debugf("level pool write: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Debugf("level pool close: %v", err)
	}
	w.Reset(io.Discard)
	pool.Put(w)
	return buf.Bytes()
}
