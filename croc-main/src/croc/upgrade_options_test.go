package croc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/schollz/croc/v11/src/telemetry"
)

func TestParseWorkerThrottle(t *testing.T) {
	id, rate, err := parseWorkerThrottle("2:512k")
	if err != nil || id != 2 || rate != 512*1024 {
		t.Fatalf("parseWorkerThrottle = %d, %d, %v", id, rate, err)
	}
	for _, value := range []string{"", "0", "512k", ":512k", "0:", "-1:512k", "a:512k", "0:fast", "0:0"} {
		if _, _, err := parseWorkerThrottle(value); err == nil {
			t.Errorf("parseWorkerThrottle(%q) succeeded; want an error", value)
		}
	}
}

func TestNewUpgradeOptions(t *testing.T) {
	options := func(change func(*Options)) Options {
		ops := Options{IsSender: true, SharedSecret: "1234-upgrade-options", Curve: "p256"}
		change(&ops)
		return ops
	}

	client, err := New(options(func(*Options) {}))
	if err != nil {
		t.Fatal(err)
	}
	if client.metrics != telemetry.Nop {
		t.Errorf("metrics without MetricsOut = %v, want Nop", client.metrics)
	}

	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	client, err = New(options(func(o *Options) { o.MetricsOut = path; o.SchedMode = "static" }))
	if err != nil {
		t.Fatal(err)
	}
	client.metrics.Emit(telemetry.MerkleVerify{CorruptChunks: 1})
	client.closeMetrics()
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Errorf("metrics file not written: %v", err)
	}

	for name, change := range map[string]func(*Options){
		"scheduler": func(o *Options) { o.SchedMode = "magic" },
		"throttle":  func(o *Options) { o.DebugThrottleWorker = "magic" },
		"metrics":   func(o *Options) { o.MetricsOut = filepath.Join(t.TempDir(), "no", "dir") },
	} {
		if _, err := New(options(change)); err == nil {
			t.Errorf("%s: New succeeded; want an error", name)
		}
	}
}
