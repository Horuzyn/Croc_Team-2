package compress

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLevelPool_RoundTrip(t *testing.T) {
	lp := NewLevelPool()
	src := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog, repeated many times "), 512)

	for _, level := range []int{flate.HuffmanOnly, flate.BestSpeed, flate.DefaultCompression} {
		compressed := lp.CompressTo(nil, src, level)
		assert.Less(t, len(compressed), len(src), "level %d should compress text", level)

		decompressed, err := DecompressTo(nil, compressed, int64(len(src)))
		require.NoError(t, err)
		assert.Equal(t, src, decompressed, "round-trip failed at level %d", level)
	}
}

func TestLevelPool_ReuseBuffer(t *testing.T) {
	lp := NewLevelPool()
	src := bytes.Repeat([]byte("repeated data for reuse test "), 256)

	dst := make([]byte, 0, len(src))
	for i := 0; i < 10; i++ {
		dst = lp.CompressTo(dst, src, flate.HuffmanOnly)
		decompressed, err := DecompressTo(nil, dst, int64(len(src)))
		require.NoError(t, err)
		assert.Equal(t, src, decompressed)
	}
}

func TestLevelPool_ConcurrentAccess(t *testing.T) {
	lp := NewLevelPool()
	src := make([]byte, 32*1024)
	_, err := rand.Read(src)
	require.NoError(t, err)

	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprintf("worker-%d", i), func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, 0, len(src)+128)
			for j := 0; j < 50; j++ {
				buf = lp.CompressTo(buf, src, flate.BestSpeed)
				decompressed, decErr := DecompressTo(nil, buf, int64(len(src))*2)
				require.NoError(t, decErr)
				assert.Equal(t, src, decompressed)
			}
		})
	}
}

func BenchmarkLevelPoolCompress(b *testing.B) {
	lp := NewLevelPool()
	data := bytes.Repeat([]byte("benchmark data for compress pool test "), 1024)[:32*1024]

	levels := []struct {
		name  string
		level int
	}{
		{"HuffmanOnly", flate.HuffmanOnly},
		{"BestSpeed", flate.BestSpeed},
		{"Default", flate.DefaultCompression},
	}

	for _, tc := range levels {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			dst := make([]byte, 0, len(data))
			// Warm up the pool before measuring allocations.
			dst = lp.CompressTo(dst, data, tc.level)
			b.ResetTimer()
			for range b.N {
				dst = lp.CompressTo(dst, data, tc.level)
			}
		})
	}
}
