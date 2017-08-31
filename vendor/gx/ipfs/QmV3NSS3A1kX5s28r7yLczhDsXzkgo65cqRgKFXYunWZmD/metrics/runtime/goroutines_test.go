package runtime

import (
	"testing"

	"gx/ipfs/QmV3NSS3A1kX5s28r7yLczhDsXzkgo65cqRgKFXYunWZmD/metrics"
)

func TestGoroutinesStats(t *testing.T) {
	_, gauges := metrics.Snapshot()

	expected := []string{
		"Goroutines.Num",
	}

	for _, name := range expected {
		if _, ok := gauges[name]; !ok {
			t.Errorf("Missing gauge %q", name)
		}
	}
}
