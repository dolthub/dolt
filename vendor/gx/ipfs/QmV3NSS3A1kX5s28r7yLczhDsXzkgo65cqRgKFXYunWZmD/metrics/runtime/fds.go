// +build !windows

package runtime

import (
	"io/ioutil"
	"syscall"

	"gx/ipfs/QmV3NSS3A1kX5s28r7yLczhDsXzkgo65cqRgKFXYunWZmD/metrics"
)

func getFDLimit() (uint64, error) {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return 0, err
	}
	// rlimit.Cur's type is platform-dependent, so here we widen it as far as Go
	// will allow by converting it to a uint64.
	return uint64(rlimit.Cur), nil
}

func getFDUsage() (uint64, error) {
	fds, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	return uint64(len(fds)), nil
}

func init() {
	metrics.Gauge("FileDescriptors.Max").SetFunc(func() int64 {
		v, err := getFDLimit()
		if err != nil {
			return 0
		}
		return int64(v)
	})

	metrics.Gauge("FileDescriptors.Used").SetFunc(func() int64 {
		v, err := getFDUsage()
		if err != nil {
			return 0
		}
		return int64(v)
	})
}
