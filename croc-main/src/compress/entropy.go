package compress

import "math"

const defaultEntropyStride = 4

// EntropyEstimate returns the estimated Shannon entropy of data in bits per byte [0, 8].
// It samples every 4th byte to keep CPU overhead well below 2% of a
// HuffmanOnly compression pass on a 32 KiB chunk.
func EntropyEstimate(data []byte) float64 {
	return entropyStride(data, defaultEntropyStride)
}

func entropyStride(data []byte, stride int) float64 {
	if len(data) == 0 {
		return 0
	}
	if stride <= 0 {
		stride = 1
	}

	var freq [256]uint32
	var n uint32
	for i := 0; i < len(data); i += stride {
		freq[data[i]]++
		n++
	}
	if n == 0 {
		return 0
	}

	invN := 1.0 / float64(n)
	var entropy float64
	for _, count := range freq {
		if count == 0 {
			continue
		}
		p := float64(count) * invN
		entropy -= p * math.Log2(p)
	}
	return entropy
}
