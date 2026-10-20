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
	"golang.org/x/sys/windows"
)

// ProcessExists reports whether a process with pid is running.
//
// On Windows, a process object can outlive the process while another
// handle remains open. A zero-timeout [WaitForSingleObject] distinguishes
// running processes from terminated processes whose objects still exist.
// The handle is closed via [CloseHandle] to avoid leaking kernel resources.
//
// [CloseHandle]: https://learn.microsoft.com/windows/win32/api/handleapi/nf-handleapi-closehandle
// [WaitForSingleObject]: https://learn.microsoft.com/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject
//
// [OpenProcess]: https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess
func ProcessExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	status, err := windows.WaitForSingleObject(handle, 0)
	return err == nil && status == uint32(windows.WAIT_TIMEOUT)
}
