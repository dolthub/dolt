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

package sqlserver

import (
	iofs "io/fs"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func getDeadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("go", "version")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Wait())
	return pid
}

func TestProcessExists(t *testing.T) {
	require.True(t, ProcessExists(os.Getpid()))
	require.False(t, ProcessExists(-1))
	require.False(t, ProcessExists(0))

	deadPID := getDeadPID(t)
	require.False(t, ProcessExists(deadPID))
}

func TestFindAndLoadLocalCredsStaleCleanup(t *testing.T) {
	fs := filesys.EmptyInMemFS(t.TempDir())
	deadPID := getDeadPID(t)
	staleCreds := &LocalCreds{Pid: deadPID, Port: 3306, Secret: "secret"}
	err := WriteLocalCreds(fs, staleCreds)
	require.NoError(t, err)

	// Dead PID credentials should be cleaned up automatically.
	creds, err := FindAndLoadLocalCreds(fs)
	require.NoError(t, err)
	require.Nil(t, creds)

	// Verify file was deleted.
	_, err = LoadLocalCreds(fs)
	require.Error(t, err)
	require.ErrorIs(t, err, iofs.ErrNotExist)

	// Subsequent lookup cleanly succeeds with nil creds (recovered).
	creds, err = FindAndLoadLocalCreds(fs)
	require.NoError(t, err)
	require.Nil(t, creds)
}

func TestFindAndLoadLocalCredsLivePID(t *testing.T) {
	fs := filesys.EmptyInMemFS(t.TempDir())
	liveCreds := &LocalCreds{Pid: os.Getpid(), Port: 3306, Secret: "secret"}
	err := WriteLocalCreds(fs, liveCreds)
	require.NoError(t, err)

	creds, err := FindAndLoadLocalCreds(fs)
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Equal(t, liveCreds.Pid, creds.Pid)
	require.Equal(t, liveCreds.Port, creds.Port)
	require.Equal(t, liveCreds.Secret, creds.Secret)
}

func TestLoadLocalCredsInvalidFormat(t *testing.T) {
	fs := filesys.EmptyInMemFS(t.TempDir())
	filePath, err := fs.Abs(LocalCredsFilePath())
	require.NoError(t, err)
	err = fs.WriteFile(filePath, []byte("invalid:format"), 0600)
	require.NoError(t, err)

	_, err = LoadLocalCreds(fs)
	require.ErrorIs(t, err, ErrInvalidLockFileFormat)
}
