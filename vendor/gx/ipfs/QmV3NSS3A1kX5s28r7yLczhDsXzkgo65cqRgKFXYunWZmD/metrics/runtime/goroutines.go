package runtime

import (
	"runtime"

	"gx/ipfs/QmV3NSS3A1kX5s28r7yLczhDsXzkgo65cqRgKFXYunWZmD/metrics"
)

func init() {
	metrics.Gauge("Goroutines.Num").SetFunc(func() int64 {
		return int64(runtime.NumGoroutine())
	})
}
