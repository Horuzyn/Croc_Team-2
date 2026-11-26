package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/croc"
	"github.com/stretchr/testify/require"
)

// payload returns a file to send. --text would leave a temp file in the package
// folder on Windows.
func payload(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "payload.txt")
	require.NoError(t, os.WriteFile(file, []byte("hello"), 0o600))
	return file
}

// rememberedSend runs a send that fails to reach its relay and returns the
// options that --remember saved just before the transfer started.
func rememberedSend(t *testing.T, flags ...string) croc.Options {
	t.Helper()
	configDirectory := t.TempDir()
	t.Setenv("CROC_CONFIG_DIR", configDirectory)
	t.Setenv("CROC_SECRET", "1234-upgrade-flags")
	args := append([]string{
		"croc", "--remember", "--ignore-stdin", "--disable-clipboard",
		"--no-update-check", "--relay", "127.0.0.1:1",
	}, flags...)
	args = append(args, "send", "--no-local", payload(t))
	require.Error(t, newApp().Run(args))

	saved, err := os.ReadFile(filepath.Join(configDirectory, "send.json"))
	require.NoError(t, err)
	var options croc.Options
	require.NoError(t, json.Unmarshal(saved, &options))
	return options
}

func TestUpgradeFlagDefaults(t *testing.T) {
	options := rememberedSend(t)
	require.False(t, options.AdaptiveCompress)
	require.False(t, options.MerkleResume)
	require.Equal(t, "dynamic", options.SchedMode)
	require.Empty(t, options.MetricsOut)
	require.Empty(t, options.DebugThrottleWorker)
}

func TestUpgradeFlagsReachSendOptions(t *testing.T) {
	metrics := filepath.Join(t.TempDir(), "metrics.jsonl")
	options := rememberedSend(t,
		"--adaptive-compress", "--sched", "static", "--merkle-resume",
		"--metrics-out", metrics, "--debug-throttle-worker", "0:512k",
	)
	require.True(t, options.AdaptiveCompress)
	require.True(t, options.MerkleResume)
	require.Equal(t, "static", options.SchedMode)
	require.Equal(t, metrics, options.MetricsOut)
	require.Equal(t, "0:512k", options.DebugThrottleWorker)
}

func TestUpgradeFlagsAreValidatedOnBothSides(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no", "metrics.jsonl")
	for _, test := range []struct{ flag, value, want string }{
		{"--sched", "magic", `invalid scheduler "magic"`},
		{"--debug-throttle-worker", "magic", "invalid worker throttle"},
		{"--metrics-out", missing, "open metrics file"},
	} {
		for side, command := range map[string][]string{
			"send":    {"send", "--no-local", payload(t)},
			"receive": nil,
		} {
			t.Run(side+test.flag, func(t *testing.T) {
				t.Setenv("CROC_CONFIG_DIR", t.TempDir())
				t.Setenv("CROC_SECRET", "1234-upgrade-flags")
				args := []string{
					"croc", "--ignore-stdin", "--disable-clipboard", "--no-update-check",
					"--yes", "--relay", "127.0.0.1:1", test.flag, test.value,
				}
				err := newApp().Run(append(args, command...))
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("error = %v, want %q", err, test.want)
				}
			})
		}
	}
}
