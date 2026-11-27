package compress

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntropyEstimate_PureText(t *testing.T) {
	data := bytes.Repeat([]byte("the quick brown fox jumps over "), 1024)
	e := EntropyEstimate(data[:32*1024])
	assert.Greater(t, e, 3.5, "text should have moderate entropy")
	assert.Less(t, e, 5.5, "text should not be near random")
}

func TestEntropyEstimate_JSONLog(t *testing.T) {
	line := []byte(`{"ts":"2026-11-27T10:00:00Z","level":"info","msg":"chunk sent","offset":32768,"bytes":32768}` + "\n")
	data := bytes.Repeat(line, 400)[:32*1024]
	e := EntropyEstimate(data)
	assert.Greater(t, e, 4.0, "JSON/log should have moderate entropy")
	assert.Less(t, e, 6.2, "JSON/log should be compressible")
}

func TestEntropyEstimate_Random(t *testing.T) {
	data := make([]byte, 32*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)
	e := EntropyEstimate(data)
	assert.Greater(t, e, 7.8, "random data should have near-maximum entropy")
}

func TestEntropyEstimate_AllZeros(t *testing.T) {
	data := make([]byte, 32*1024)
	e := EntropyEstimate(data)
	assert.Equal(t, 0.0, e, "uniform data should have zero entropy")
}

func TestEntropyEstimate_SingleByte(t *testing.T) {
	e := EntropyEstimate([]byte{42})
	assert.Equal(t, 0.0, e, "single distinct value = zero entropy")
}

func TestEntropyEstimate_Empty(t *testing.T) {
	assert.Equal(t, 0.0, EntropyEstimate(nil))
	assert.Equal(t, 0.0, EntropyEstimate([]byte{}))
}

func TestEntropyEstimate_Compressed(t *testing.T) {
	// Compressed payloads (e.g. .zip/.mp4) exhibit high byte entropy (> 7.5).
	raw := bytes.Repeat([]byte("highly-compressible-structured-payload-"), 1024)
	compressed := Compress(raw[:32*1024])
	e := EntropyEstimate(compressed)
	assert.Greater(t, e, 7.0, "compressed stream should have high entropy")
}

var entropyBenchSink float64

func BenchmarkEntropyEstimate(b *testing.B) {
	data := make([]byte, 32*1024)
	_, err := rand.Read(data)
	require.NoError(b, err)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum float64
	for range b.N {
		sum += EntropyEstimate(data)
	}
	entropyBenchSink = sum
}
