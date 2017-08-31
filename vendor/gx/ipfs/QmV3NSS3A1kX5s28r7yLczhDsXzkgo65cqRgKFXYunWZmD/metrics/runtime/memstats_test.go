package runtime

import (
	"testing"

	"gx/ipfs/QmV3NSS3A1kX5s28r7yLczhDsXzkgo65cqRgKFXYunWZmD/metrics"
)

func TestMemStats(t *testing.T) {
	counters, gauges := metrics.Snapshot()

	expectedCounters := []string{
		"Mem.NumGC",
		"Mem.PauseTotalNs",
	}

	expectedGauges := []string{
		"Mem.LastGC",
		"Mem.Alloc",
		"Mem.HeapObjects",
		"Mem.NextGC",
	}

	for _, name := range expectedCounters {
		if _, ok := counters[name]; !ok {
			t.Errorf("Missing counters %q", name)
		}
	}

	for _, name := range expectedGauges {
		if _, ok := gauges[name]; !ok {
			t.Errorf("Missing gauge %q", name)
		}
	}
}
