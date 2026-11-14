package chunkmeta

import (
	"math"
	"testing"
	"time"
)

func TestNamespacePreservesExistingCacheIdentity(t *testing.T) {
	// These identities were captured before moving the policy. Keep existing
	// records and metric history usable across this maintenance refactor.
	for _, tc := range []struct{ prefix, want string }{
		{"/test//", "67958fd4b2a1cddaf7c47ecd1b0bf488f2e9b70617c340d2cab190014f8ddaea"},
		{"", "e0e7b873ddf0b9ff3047e643d89088c16e5861811a01846d657c7c4a118d7630"},
	} {
		if got := Namespace("https://test", "region", "bucket", tc.prefix, true); got != tc.want {
			t.Fatalf("namespace changed for prefix %q: %s", tc.prefix, got)
		}
	}
}

func TestRefreshIntervalBounds(t *testing.T) {
	for _, qps := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1), 1e-20} {
		if _, err := RefreshInterval(qps); err == nil {
			t.Fatalf("accepted invalid qps %g", qps)
		}
	}
	for _, tc := range []struct {
		qps  float64
		want time.Duration
	}{{2, 500 * time.Millisecond}, {0.1, 10 * time.Second}, {1e12, time.Nanosecond}} {
		if got, err := RefreshInterval(tc.qps); err != nil || got != tc.want {
			t.Fatalf("qps %g: interval=%s error=%v", tc.qps, got, err)
		}
	}
}
