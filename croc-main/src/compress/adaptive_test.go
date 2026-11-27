package compress

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptive_PureText(t *testing.T) {
	lp := NewLevelPool()
	src := bytes.Repeat(fable, 16)[:32*1024]

	res := AdaptiveCompressChunk(lp, nil, src, flate.HuffmanOnly)
	assert.Equal(t, CodecFlate, res.Codec)
	assert.Less(t, len(res.Data), len(src))
	assert.Less(t, res.Entropy, 6.0)

	decompressed, err := DecompressTo(nil, res.Data, int64(len(src)))
	require.NoError(t, err)
	assert.Equal(t, src, decompressed)
}

func TestAdaptive_JSONLog(t *testing.T) {
	lp := NewLevelPool()
	line := []byte(`{"ts":"2026-11-27T10:00:00Z","level":"info","msg":"chunk sent","offset":32768,"bytes":32768}` + "\n")
	src := bytes.Repeat(line, 400)[:32*1024]

	res := AdaptiveCompressChunk(lp, nil, src, flate.BestSpeed)
	assert.Equal(t, CodecFlate, res.Codec)
	assert.Greater(t, res.Entropy, 4.0)
	assert.Less(t, res.Entropy, 6.2)

	decompressed, err := DecompressTo(nil, res.Data, int64(len(src)))
	require.NoError(t, err)
	assert.Equal(t, src, decompressed)
}

func TestAdaptive_PreCompressed(t *testing.T) {
	lp := NewLevelPool()
	raw, err := os.ReadFile("../croc/croc.go")
	require.NoError(t, err)
	src := CompressWithOption(raw, 6)
	if len(src) > 32*1024 {
		src = src[:32*1024]
	}

	res := AdaptiveCompressChunk(lp, nil, src, flate.HuffmanOnly)
	assert.Equal(t, CodecRaw, res.Codec)
	assert.Greater(t, res.Entropy, EntropySkipThreshold)
	assert.Equal(t, src, res.Data)
}

func TestAdaptive_Random(t *testing.T) {
	lp := NewLevelPool()
	src := make([]byte, 32*1024)
	_, err := rand.Read(src)
	require.NoError(t, err)

	res := AdaptiveCompressChunk(lp, nil, src, flate.HuffmanOnly)
	assert.Equal(t, CodecRaw, res.Codec)
	assert.Greater(t, res.Entropy, 7.8)
	assert.Equal(t, src, res.Data)
}

func TestAdaptive_SkipRatio(t *testing.T) {
	lp := NewLevelPool()
	const totalChunks = 1000
	chunk := make([]byte, 32*1024)
	var dst []byte
	skipped := 0

	for range totalChunks {
		_, err := rand.Read(chunk)
		require.NoError(t, err)
		res := AdaptiveCompressChunk(lp, dst, chunk, flate.HuffmanOnly)
		if res.Codec == CodecRaw {
			skipped++
		} else {
			dst = res.Data
		}
	}

	skipRate := float64(skipped) / float64(totalChunks)
	assert.Greater(t, skipRate, 0.99, "random chunks skip rate must exceed 99%")
}

func TestAdaptive_MarginalCompression(t *testing.T) {
	lp := NewLevelPool()
	// 128 distinct bytes: sampled entropy <= 7.5 (hits slow path),
	// but Huffman table overhead makes compressed output >= 0.96 * len(src).
	src := make([]byte, 128)
	for i := range src {
		src[i] = byte(i)
	}

	res := AdaptiveCompressChunk(lp, nil, src, flate.HuffmanOnly)
	assert.LessOrEqual(t, res.Entropy, EntropySkipThreshold)
	assert.Equal(t, CodecRaw, res.Codec)
	assert.Equal(t, src, res.Data)
}

func TestAdaptive_Empty(t *testing.T) {
	lp := NewLevelPool()
	res := AdaptiveCompressChunk(lp, nil, []byte{}, flate.HuffmanOnly)
	assert.Equal(t, CodecRaw, res.Codec)
	assert.Equal(t, 0.0, res.Entropy)
	assert.Empty(t, res.Data)
}

func TestShouldSkipCompression(t *testing.T) {
	text := bytes.Repeat(fable, 16)[:32*1024]
	assert.False(t, ShouldSkipCompression(text))

	rnd := make([]byte, 32*1024)
	_, err := rand.Read(rnd)
	require.NoError(t, err)
	assert.True(t, ShouldSkipCompression(rnd))
}

var adaptiveBenchSink ChunkResult

func BenchmarkAdaptiveCompressChunk(b *testing.B) {
	lp := NewLevelPool()
	text := bytes.Repeat(fable, 16)[:32*1024]
	rnd := make([]byte, 32*1024)
	_, err := rand.Read(rnd)
	require.NoError(b, err)

	b.Run("Text-CodecFlate", func(b *testing.B) {
		b.SetBytes(int64(len(text)))
		b.ReportAllocs()
		dst := make([]byte, 0, len(text))
		for range b.N {
			res := AdaptiveCompressChunk(lp, dst, text, flate.HuffmanOnly)
			dst = res.Data
			adaptiveBenchSink = res
		}
	})

	b.Run("Random-CodecRawFastPath", func(b *testing.B) {
		b.SetBytes(int64(len(rnd)))
		b.ReportAllocs()
		dst := make([]byte, 0, len(rnd))
		for range b.N {
			adaptiveBenchSink = AdaptiveCompressChunk(lp, dst, rnd, flate.HuffmanOnly)
		}
	})
}
