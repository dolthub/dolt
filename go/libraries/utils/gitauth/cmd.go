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

package gitauth

import (
	"os/exec"
	"time"
)

// CmdWaitDelay bounds how long Wait blocks on output pipes that are still open
// after git itself is gone. git's transport runs as a grandchild (ssh, or
// git-remote-https -> git send-pack) holding the pipe we read, so if it outlives
// git there is no EOF to wait for. The delay elapses only when something is still
// holding the pipe; a healthy run sees EOF as the processes exit.
const CmdWaitDelay = 10 * time.Second

// CmdKillGroupOnCancel makes cancellation of |cmd|'s context kill the whole
// process group, so the transport processes git spawned go away with it.
// exec.CommandContext's default Cancel signals one pid and leaves them running.
//
// Must be paired with CmdSetsid, which makes the child the group leader. Callers
// should also set cmd.WaitDelay: a group kill cannot reach a holder that left the
// group, such as an ssh ControlMaster.
func CmdKillGroupOnCancel(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		return killProcessGroup(cmd)
	}
}
