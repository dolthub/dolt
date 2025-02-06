// Copyright 2025 Dolthub, Inc.
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
// +build windows

package binlogreplication

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func ApplyCmdAttributes(cmd *exec.Cmd) {
	// Creating a new process group for the process will allow GracefulStop to send the break signal to that process
	// without also killing the parent process
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

}

func StopProcess(proc *os.Process) error {
	err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(proc.Pid))
	if err != nil {
		return err
	}
	_, err = proc.Wait()
	return err
}

// I don't know if there is any magic necessary here, but regardless,
// we don't run these tests on windows, so there are never child
// mysqld processes to worry about.
func InstallSignalHandlers() {
}
