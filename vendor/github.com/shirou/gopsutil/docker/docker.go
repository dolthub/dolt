package docker

import (
	"errors"

	"github.com/shirou/gopsutil/internal/common"
)

var ErrDockerNotAvailable = errors.New("docker not available")
var ErrCgroupNotAvailable = errors.New("cgroup not available")

var invoke common.Invoker

func init() {
	invoke = common.Invoke{}
}

type CgroupMemStat struct {
	ContainerID             string `json:"containerID"`
	Cache                   uint64 `json:"cache"`
	RSS                     uint64 `json:"rss"`
	RSSHuge                 uint64 `json:"rssHuge"`
	MappedFile              uint64 `json:"mappedFile"`
	Pgpgin                  uint64 `json:"pgpgin"`
	Pgpgout                 uint64 `json:"pgpgout"`
	Pgfault                 uint64 `json:"pgfault"`
	Pgmajfault              uint64 `json:"pgmajfault"`
	InactiveAnon            uint64 `json:"inactiveAnon"`
	ActiveAnon              uint64 `json:"activeAnon"`
	InactiveFile            uint64 `json:"inactiveFile"`
	ActiveFile              uint64 `json:"activeFile"`
	Unevictable             uint64 `json:"unevictable"`
	HierarchicalMemoryLimit uint64 `json:"hierarchicalMemoryLimit"`
	TotalCache              uint64 `json:"totalCache"`
	TotalRSS                uint64 `json:"totalRss"`
	TotalRSSHuge            uint64 `json:"totalRssHuge"`
	TotalMappedFile         uint64 `json:"totalMappedFile"`
	TotalPgpgIn             uint64 `json:"totalPgpgin"`
	TotalPgpgOut            uint64 `json:"totalPgpgout"`
	TotalPgFault            uint64 `json:"totalPgfault"`
	TotalPgMajFault         uint64 `json:"totalPgmajfault"`
	TotalInactiveAnon       uint64 `json:"totalInactiveAnon"`
	TotalActiveAnon         uint64 `json:"totalActiveAnon"`
	TotalInactiveFile       uint64 `json:"totalInactiveFile"`
	TotalActiveFile         uint64 `json:"totalActiveFile"`
	TotalUnevictable        uint64 `json:"totalUnevictable"`
	MemUsageInBytes         uint64 `json:"memUsageInBytes"`
	MemMaxUsageInBytes      uint64 `json:"memMaxUsageInBytes"`
	MemLimitInBytes         uint64 `json:"memoryLimitInBbytes"`
	MemFailCnt              uint64 `json:"memoryFailcnt"`
}

type CgroupDockerStat struct {
	ContainerID string `json:"containerID"`
	Name        string `json:"name"`
	Image       string `json:"image"`
	Status      string `json:"status"`
	Running     bool   `json:"running"`
}
