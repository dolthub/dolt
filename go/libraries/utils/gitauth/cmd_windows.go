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

package gitauth

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

// CmdSetsid detaches |cmd| from the parent console (DETACHED_PROCESS).
// SSH needs CONIN$ to prompt for credentials; without a console it exits with an auth error.
func CmdSetsid(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.DETACHED_PROCESS,
	}
}

// killProcessGroup kills |cmd| and its descendants. Windows has no group to
// signal -- TerminateProcess takes one process, and DETACHED_PROCESS leaves no
// console for GenerateConsoleCtrlEvent -- so use `taskkill /T`, which walks the
// tree down from git while git is still alive.
func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	kill := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
	kill.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	if err := kill.Run(); err != nil {
		// taskkill is unavailable or the pid is already gone: stop git itself so
		// cancellation still takes effect, and let WaitDelay cover its descendants.
		return cmd.Process.Kill()
	}
	return nil
}
