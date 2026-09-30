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

//go:build windows

package dbfactory

import (
	"errors"
	"time"

	"golang.org/x/sys/windows"
)

// ErrProcUnavailable indicates that process start time cannot be
// read from the operating system.
var ErrProcUnavailable = errors.New("process start time unavailable")

// processStartTime returns the start time of the process |pid|.
//
// On Windows, processStartTime invokes the Win32 [OpenProcess] API
// followed by [GetProcessTimes] to inspect process creation time.
//
// [OpenProcess]: https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess
// [GetProcessTimes]: https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes
func processStartTime(pid int) (time.Time, error) {
	if pid <= 0 {
		return time.Time{}, errors.New("invalid pid")
	}
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return time.Time{}, ErrProcUnavailable
		}
		return time.Time{}, err
	}
	defer windows.CloseHandle(h)

	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(h, &creation, &exit, &kernel, &user); err != nil {
		return time.Time{}, ErrProcUnavailable
	}
	return time.Unix(0, creation.Nanoseconds()), nil
}
