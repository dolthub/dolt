package sysinfo

import (
	"fmt"
	"syscall"
)

func init() {
	diskUsageImpl = darwinDiskUsage
	memInfoImpl = darwinMemInfo
}

func darwinDiskUsage(path string) (*DiskStats, error) {
	var stfst syscall.Statfs_t
	err := syscall.Statfs(path, &stfst)
	if err != nil {
		return nil, err
	}

	free := stfst.Bfree * uint64(stfst.Bsize)
	total := stfst.Bavail * uint64(stfst.Bsize)
	return &DiskStats{
		Free:   free,
		Total:  total,
		FsType: fmt.Sprint(stfst.Type),
	}, nil
}

func darwinMemInfo() (*MemStats, error) {
	// TODO: use vm_stat on osx to gather memory information
	return new(MemStats), nil
}
