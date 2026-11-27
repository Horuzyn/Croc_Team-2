package message

import "testing"

// The wire names are a contract between team members; changing one breaks peers.
func TestUpgradeContractNames(t *testing.T) {
	for got, want := range map[string]string{
		string(TypeMerkleManifest):     "merkle-manifest",
		string(TypeChunkRepairRequest): "chunk-repair-req",
		string(TypeChunkRepairAck):     "chunk-repair-ack",
		FeatureAdaptiveChunkCompressV1: "adaptive-chunk-compress-v1",
		FeatureDynamicChunkSchedV1:     "dynamic-chunk-sched-v1",
		FeatureMerkleChunkResumeV1:     "merkle-chunk-resume-v1",
	} {
		if got != want {
			t.Errorf("wire name = %q, want %q", got, want)
		}
	}
}

func TestUpgradeMessagesRoundTrip(t *testing.T) {
	for _, kind := range []Type{TypeMerkleManifest, TypeChunkRepairRequest, TypeChunkRepairAck} {
		encoded, err := Encode(nil, Message{Type: kind, Bytes: []byte("payload"), Num: 3})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(nil, encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Type != kind || string(decoded.Bytes) != "payload" || decoded.Num != 3 {
			t.Errorf("round trip of %s = %+v", kind, decoded)
		}
	}
}
