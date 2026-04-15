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

package gitauth

import (
	"os/exec"
	"syscall"
)

// CmdSetsid places |cmd| in a new session with no controlling terminal by
// setting Setsid on [syscall.SysProcAttr]. SSH requires /dev/tty to prompt
// for a passphrase; the open fails without a controlling terminal and SSH
// exits with an authentication error that [NormalizeError] converts into a
// [NonInteractiveAuthError].
//
// See https://man7.org/linux/man-pages/man2/setsid.2.html
func CmdSetsid(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
