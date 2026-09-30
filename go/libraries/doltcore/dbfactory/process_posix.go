// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows

package dbfactory

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrProcUnavailable indicates that process start time cannot be
// read from the operating system.
var ErrProcUnavailable = errors.New("process start time unavailable")

// processStartTime returns the start time of the process |pid|.
//
// On Linux, processStartTime reads field 22 (starttime) from
// /proc/<pid>/stat. If /proc is unavailable (such as on Darwin or
// BSD), processStartTime tests process liveness via signal 0 per
// [POSIX.1-2017 kill]. If the process does not exist, it returns
// [os.ErrNotExist]. Otherwise it returns ErrProcUnavailable.
//
// [POSIX.1-2017 kill]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/kill.html
func processStartTime(pid int) (time.Time, error) {
	if pid <= 0 {
		return time.Time{}, errors.New("invalid pid")
	}
	t, err := linuxProcessStartTime(pid)
	if err == nil {
		return t, nil
	}
	if !processExists(pid) {
		return time.Time{}, os.ErrNotExist
	}
	return time.Time{}, ErrProcUnavailable
}

const (
	// userHz defines the USER_HZ clock ticks per second on Linux systems.
	userHz = 100
	// starttimeFieldIndex defines the 0-indexed position of starttime in /proc/<pid>/stat.
	starttimeFieldIndex = 19
	// tickNanoseconds defines the duration of each clock tick in nanoseconds.
	tickNanoseconds = int64(time.Second / userHz)
)

// linuxProcessStartTime reads the start time of |pid| from
// /proc/<pid>/stat and /proc/stat btime.
func linuxProcessStartTime(pid int) (time.Time, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return time.Time{}, err
	}
	s := string(data)
	rParen := strings.LastIndex(s, ")")
	if rParen == -1 || rParen+2 >= len(s) {
		return time.Time{}, errors.New("malformed stat")
	}
	fields := strings.Fields(s[rParen+2:])
	if len(fields) <= starttimeFieldIndex {
		return time.Time{}, errors.New("truncated stat")
	}
	ticks, err := strconv.ParseInt(fields[starttimeFieldIndex], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	btime, err := readLinuxBtime()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(btime+ticks/userHz, (ticks%userHz)*tickNanoseconds), nil
}

// readLinuxBtime reads the system boot time from /proc/stat.
func readLinuxBtime() (int64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "btime ") {
			return strconv.ParseInt(strings.TrimSpace(line[6:]), 10, 64)
		}
	}
	return 0, errors.New("btime not found")
}

// processExists reports whether a process with |pid| is running.
func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
