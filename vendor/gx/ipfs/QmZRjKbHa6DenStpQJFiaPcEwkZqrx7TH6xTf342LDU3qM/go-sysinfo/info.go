package sysinfo

import (
	"errors"
)

var ErrPlatformNotSupported = errors.New("this operation is not supported on your platform")

type DiskStats struct {
	Free   uint64
	Total  uint64
	FsType string
}

var diskUsageImpl func(string) (*DiskStats, error)

func DiskUsage(path string) (*DiskStats, error) {
	if diskUsageImpl == nil {
		return nil, ErrPlatformNotSupported
	}

	return diskUsageImpl(path)
}

type MemStats struct {
	Swap uint64
	Used uint64
}

var memInfoImpl func() (*MemStats, error)

func MemoryInfo() (*MemStats, error) {
	if memInfoImpl == nil {
		return nil, ErrPlatformNotSupported
	}

	return memInfoImpl()
}
