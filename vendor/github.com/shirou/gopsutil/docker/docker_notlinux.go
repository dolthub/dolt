// +build !linux

package docker

import (
	"encoding/json"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/internal/common"
)

// GetDockerStat returns a list of Docker basic stats.
// This requires certain permission.
func GetDockerStat() ([]CgroupDockerStat, error) {
	return nil, ErrDockerNotAvailable
}

// GetDockerIDList returnes a list of DockerID.
// This requires certain permission.
func GetDockerIDList() ([]string, error) {
	return nil, ErrDockerNotAvailable
}

// CgroupCPU returnes specified cgroup id CPU status.
// containerid is same as docker id if you use docker.
// If you use container via systemd.slice, you could use
// containerid = docker-<container id>.scope and base=/sys/fs/cgroup/cpuacct/system.slice/
func CgroupCPU(containerid string, base string) (*cpu.TimesStat, error) {
	return nil, ErrCgroupNotAvailable
}

func CgroupCPUDocker(containerid string) (*cpu.TimesStat, error) {
	return CgroupCPU(containerid, common.HostSys("fs/cgroup/cpuacct/docker"))
}

func CgroupMem(containerid string, base string) (*CgroupMemStat, error) {
	return nil, ErrCgroupNotAvailable
}

func CgroupMemDocker(containerid string) (*CgroupMemStat, error) {
	return CgroupMem(containerid, common.HostSys("fs/cgroup/memory/docker"))
}

func (m CgroupMemStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}
