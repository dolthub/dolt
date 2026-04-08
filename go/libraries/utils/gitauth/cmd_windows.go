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
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

// CmdSetsid detaches |cmd| from the parent console by setting
// [windows.DETACHED_PROCESS] on [syscall.SysProcAttr]. Without a console,
// the MSYS2 ssh binary cannot open CONIN$ to prompt for a passphrase and
// exits with an authentication error.
func CmdSetsid(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.DETACHED_PROCESS,
	}
}
