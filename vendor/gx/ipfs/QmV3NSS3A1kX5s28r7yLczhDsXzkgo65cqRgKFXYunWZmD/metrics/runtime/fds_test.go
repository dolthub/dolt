// +build !windows

package runtime

import (
	"testing"

	"gx/ipfs/QmV3NSS3A1kX5s28r7yLczhDsXzkgo65cqRgKFXYunWZmD/metrics"
)

func TestFdStats(t *testing.T) {
	_, gauges := metrics.Snapshot()

	expected := []string{
		"FileDescriptors.Max",
		"FileDescriptors.Used",
	}

	for _, name := range expected {
		if _, ok := gauges[name]; !ok {
			t.Errorf("Missing gauge %q", name)
		}
	}
}
