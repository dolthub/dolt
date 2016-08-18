// +build linux

package docker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	cpu "github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/internal/common"
)

// GetDockerStat returns a list of Docker basic stats.
// This requires certain permission.
func GetDockerStat() ([]CgroupDockerStat, error) {
	path, err := exec.LookPath("docker")
	if err != nil {
		return nil, ErrDockerNotAvailable
	}

	out, err := invoke.Command(path, "ps", "-a", "--no-trunc", "--format", "{{.ID}}|{{.Image}}|{{.Names}}|{{.Status}}")
	if err != nil {
		return []CgroupDockerStat{}, err
	}
	lines := strings.Split(string(out), "\n")
	ret := make([]CgroupDockerStat, 0, len(lines))

	for _, l := range lines {
		if l == "" {
			continue
		}
		cols := strings.Split(l, "|")
		if len(cols) != 4 {
			continue
		}
		names := strings.Split(cols[2], ",")
		stat := CgroupDockerStat{
			ContainerID: cols[0],
			Name:        names[0],
			Image:       cols[1],
			Status:      cols[3],
			Running:     strings.Contains(cols[3], "Up"),
		}
		ret = append(ret, stat)
	}

	return ret, nil
}

func (c CgroupDockerStat) String() string {
	s, _ := json.Marshal(c)
	return string(s)
}

// GetDockerIDList returnes a list of DockerID.
// This requires certain permission.
func GetDockerIDList() ([]string, error) {
	path, err := exec.LookPath("docker")
	if err != nil {
		return nil, ErrDockerNotAvailable
	}

	out, err := invoke.Command(path, "ps", "-q", "--no-trunc")
	if err != nil {
		return []string{}, err
	}
	lines := strings.Split(string(out), "\n")
	ret := make([]string, 0, len(lines))

	for _, l := range lines {
		if l == "" {
			continue
		}
		ret = append(ret, l)
	}

	return ret, nil
}

// CgroupCPU returnes specified cgroup id CPU status.
// containerID is same as docker id if you use docker.
// If you use container via systemd.slice, you could use
// containerID = docker-<container id>.scope and base=/sys/fs/cgroup/cpuacct/system.slice/
func CgroupCPU(containerID string, base string) (*cpu.TimesStat, error) {
	statfile := getCgroupFilePath(containerID, base, "cpuacct", "cpuacct.stat")
	lines, err := common.ReadLines(statfile)
	if err != nil {
		return nil, err
	}
	// empty containerID means all cgroup
	if len(containerID) == 0 {
		containerID = "all"
	}
	ret := &cpu.TimesStat{CPU: containerID}
	for _, line := range lines {
		fields := strings.Split(line, " ")
		if fields[0] == "user" {
			user, err := strconv.ParseFloat(fields[1], 64)
			if err == nil {
				ret.User = float64(user)
			}
		}
		if fields[0] == "system" {
			system, err := strconv.ParseFloat(fields[1], 64)
			if err == nil {
				ret.System = float64(system)
			}
		}
	}

	return ret, nil
}

func CgroupCPUDocker(containerid string) (*cpu.TimesStat, error) {
	return CgroupCPU(containerid, common.HostSys("fs/cgroup/cpuacct/docker"))
}

func CgroupMem(containerID string, base string) (*CgroupMemStat, error) {
	statfile := getCgroupFilePath(containerID, base, "memory", "memory.stat")

	// empty containerID means all cgroup
	if len(containerID) == 0 {
		containerID = "all"
	}
	lines, err := common.ReadLines(statfile)
	if err != nil {
		return nil, err
	}
	ret := &CgroupMemStat{ContainerID: containerID}
	for _, line := range lines {
		fields := strings.Split(line, " ")
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "cache":
			ret.Cache = v
		case "rss":
			ret.RSS = v
		case "rssHuge":
			ret.RSSHuge = v
		case "mappedFile":
			ret.MappedFile = v
		case "pgpgin":
			ret.Pgpgin = v
		case "pgpgout":
			ret.Pgpgout = v
		case "pgfault":
			ret.Pgfault = v
		case "pgmajfault":
			ret.Pgmajfault = v
		case "inactiveAnon":
			ret.InactiveAnon = v
		case "activeAnon":
			ret.ActiveAnon = v
		case "inactiveFile":
			ret.InactiveFile = v
		case "activeFile":
			ret.ActiveFile = v
		case "unevictable":
			ret.Unevictable = v
		case "hierarchicalMemoryLimit":
			ret.HierarchicalMemoryLimit = v
		case "totalCache":
			ret.TotalCache = v
		case "totalRss":
			ret.TotalRSS = v
		case "totalRssHuge":
			ret.TotalRSSHuge = v
		case "totalMappedFile":
			ret.TotalMappedFile = v
		case "totalPgpgin":
			ret.TotalPgpgIn = v
		case "totalPgpgout":
			ret.TotalPgpgOut = v
		case "totalPgfault":
			ret.TotalPgFault = v
		case "totalPgmajfault":
			ret.TotalPgMajFault = v
		case "totalInactiveAnon":
			ret.TotalInactiveAnon = v
		case "totalActiveAnon":
			ret.TotalActiveAnon = v
		case "totalInactiveFile":
			ret.TotalInactiveFile = v
		case "totalActiveFile":
			ret.TotalActiveFile = v
		case "totalUnevictable":
			ret.TotalUnevictable = v
		}
	}

	r, err := getCgroupMemFile(containerID, base, "memory.usage_in_bytes")
	if err == nil {
		ret.MemUsageInBytes = r
	}
	r, err = getCgroupMemFile(containerID, base, "memory.max_usage_in_bytes")
	if err == nil {
		ret.MemMaxUsageInBytes = r
	}
	r, err = getCgroupMemFile(containerID, base, "memoryLimitInBbytes")
	if err == nil {
		ret.MemLimitInBytes = r
	}
	r, err = getCgroupMemFile(containerID, base, "memoryFailcnt")
	if err == nil {
		ret.MemFailCnt = r
	}

	return ret, nil
}

func CgroupMemDocker(containerID string) (*CgroupMemStat, error) {
	return CgroupMem(containerID, common.HostSys("fs/cgroup/memory/docker"))
}

func (m CgroupMemStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}

// getCgroupFilePath constructs file path to get targetted stats file.
func getCgroupFilePath(containerID, base, target, file string) string {
	if len(base) == 0 {
		base = common.HostSys(fmt.Sprintf("fs/cgroup/%s/docker", target))
	}
	statfile := path.Join(base, containerID, file)

	if _, err := os.Stat(statfile); os.IsNotExist(err) {
		statfile = path.Join(
			common.HostSys(fmt.Sprintf("fs/cgroup/%s/system.slice", target)), "docker-"+containerID+".scope", file)
	}

	return statfile
}

// getCgroupMemFile reads a cgroup file and return the contents as uint64.
func getCgroupMemFile(containerID, base, file string) (uint64, error) {
	statfile := getCgroupFilePath(containerID, base, "memory", file)
	lines, err := common.ReadLines(statfile)
	if err != nil {
		return 0, err
	}
	if len(lines) != 1 {
		return 0, fmt.Errorf("wrong format file: %s", statfile)
	}
	return strconv.ParseUint(lines[0], 10, 64)
}
