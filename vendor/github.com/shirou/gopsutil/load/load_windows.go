// +build windows

package load

import (
	"github.com/shirou/gopsutil/internal/common"
)

func Avg() (*AvgStat, error) {
	ret := AvgStat{}

	return &ret, common.ErrNotImplementedError
}

func Misc() (*MiscStat, error) {
	ret := MiscStat{}

	return &ret, common.ErrNotImplementedError
}
