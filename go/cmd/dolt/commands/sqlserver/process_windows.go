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

package sqlserver

import (
	"os"
)

// ProcessExists reports whether a process with pid is running.
//
// On Windows, [os.FindProcess] invokes the Win32 [OpenProcess] API.
// If the process does not exist, OpenProcess fails. If it succeeds,
// [os.Process.Release] must be called to close the handle via
// [CloseHandle] and avoid leaking kernel resources per Microsoft
// documentation.
//
// [CloseHandle]: https://learn.microsoft.com/windows/win32/api/handleapi/nf-handleapi-closehandle
// [OpenProcess]: https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess
// [os.FindProcess]: https://go.dev/src/os/exec_windows.go
func ProcessExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = p.Release()
	return true
}
