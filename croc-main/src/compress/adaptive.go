package compress

// Codec identifies the compression applied to a data chunk on the wire.
type Codec byte

const (
	// CodecRaw means the chunk was sent uncompressed.
	// Receiver skips DecompressTo.
	CodecRaw Codec = 0x00

	// CodecFlate means the chunk was compressed with flate (any level).
	// Receiver calls DecompressTo.
	CodecFlate Codec = 0x01
)

// Adaptive compression thresholds.
const (
	// EntropySkipThreshold: chunks with estimated entropy above this
	// are almost certainly incompressible (random, encrypted, pre-compressed).
	EntropySkipThreshold = 7.5

	// MinCompressionSaving: if compressed size >= this fraction of raw size,
	// the saving is too small to justify decompression CPU on the receiver.
	MinCompressionSaving = 0.96
)

// ChunkResult holds the outcome of an adaptive compression decision.
type ChunkResult struct {
	Codec   Codec   // CodecRaw or CodecFlate
	Data    []byte  // the payload to send (raw or compressed)
	Entropy float64 // estimated entropy of the input chunk (bits/byte)
}

// ShouldSkipCompression returns true when entropy analysis alone indicates
// the chunk is incompressible. This is the fast path: no trial compression.
func ShouldSkipCompression(data []byte) bool {
	return EntropyEstimate(data) > EntropySkipThreshold
}

// AdaptiveCompressChunk decides per-chunk whether to compress or send raw.
//
// Fast path:  empty or entropy > 7.5 -> return raw immediately (no compress attempt).
// Slow path:  compress with LevelPool at the given level, then check ratio.
//
//	If compressed >= 96% of raw -> discard compressed, return raw.
//
// The returned ChunkResult.Data may alias dst's backing array when Codec == CodecFlate,
// or src when Codec == CodecRaw.
func AdaptiveCompressChunk(pool *LevelPool, dst, src []byte, level int) ChunkResult {
	if len(src) == 0 {
		return ChunkResult{
			Codec:   CodecRaw,
			Data:    src,
			Entropy: 0,
		}
	}

	entropy := EntropyEstimate(src)

	// Fast path: high entropy -> skip compression entirely.
	if entropy > EntropySkipThreshold {
		return ChunkResult{
			Codec:   CodecRaw,
			Data:    src,
			Entropy: entropy,
		}
	}

	// Slow path: trial compression.
	var compressed []byte
	if pool != nil {
		compressed = pool.CompressTo(dst, src, level)
	} else {
		compressed = CompressTo(dst, src)
	}

	// Check if the saving justifies receiver-side decompression CPU.
	if float64(len(compressed)) >= MinCompressionSaving*float64(len(src)) {
		return ChunkResult{
			Codec:   CodecRaw,
			Data:    src,
			Entropy: entropy,
		}
	}

	return ChunkResult{
		Codec:   CodecFlate,
		Data:    compressed,
		Entropy: entropy,
	}
}
