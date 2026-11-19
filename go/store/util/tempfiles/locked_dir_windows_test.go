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

package tempfiles

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// TestCleanDir_DeletePending holds a non-POSIX deletion pending throughout cleanup.
// Ordinary os.Remove can unlink immediately on modern Windows despite open handles.
func TestCleanDir_DeletePending(t *testing.T) {
	parent := t.TempDir()
	dir, _ := writeLockedDir(t, parent, "pending")
	lockPath := dir + lockedDirLockSuffix
	contender := otherProcess(t, dir)
	name, err := windows.UTF16PtrFromString(lockPath)
	require.NoError(t, err)
	deleting, err := windows.CreateFile(name, windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	require.NoError(t, err)
	// A handle kept in t.Cleanup remains safe if an assertion below fails.
	t.Cleanup(func() {
		if deleting != windows.InvalidHandle {
			_ = windows.CloseHandle(deleting)
		}
	})
	pending := byte(1)
	require.NoError(t, windows.SetFileInformationByHandle(deleting, windows.FileDispositionInfo,
		&pending, 1))
	require.NoError(t, windows.CloseHandle(deleting))
	deleting = windows.InvalidHandle
	// Check the fixture itself: a pending name gives access denied, rather than
	// the file-not-found result of an already unlinked name.
	probe, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err == nil {
		_ = windows.CloseHandle(probe)
	}
	require.ErrorIs(t, err, windows.ERROR_ACCESS_DENIED)
	require.NoError(t, cleanLockedDir(lockPath))
	require.NoError(t, CleanDir(parent))
	require.NoError(t, contender.Close())
	_, err = os.Stat(lockPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
