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
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestProcessExistsTerminatedProcessWithOpenHandle(t *testing.T) {
	cmd := exec.Command("go", "version")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid

	// Keep the process object alive after the process exits.
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	require.NoError(t, err)
	defer windows.CloseHandle(handle)

	require.NoError(t, cmd.Wait())
	require.False(t, ProcessExists(pid))
}
