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

package filesys

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalFSMoveDir_Windows(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	tempDir := t.TempDir()
	fs, err := LocalFilesysWithWorkingDir(tempDir)
	require.NoError(t, err)

	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest_dir")
	destFile := filepath.Join(tempDir, "dest_file")

	require.NoError(t, os.Mkdir(srcDir, 0755))
	require.NoError(t, os.Mkdir(destDir, 0755))
	require.NoError(t, os.WriteFile(destFile, []byte("contents"), 0644))

	// Move directory onto existing directory must fail quickly with [os.ErrExist]
	start := time.Now()
	err = fs.MoveDir(srcDir, destDir)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrExist), "expected os.ErrExist, got: %v", err)
	assert.Less(t, elapsed, 2*time.Second, "move must fail quickly without retrying sharing violation")

	// Move directory onto existing file must fail
	err = fs.MoveDir(srcDir, destFile)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrExist), "expected os.ErrExist, got: %v", err)

	// Verify existing file is preserved and not replaced
	fileData, err := os.ReadFile(destFile)
	require.NoError(t, err)
	assert.Equal(t, []byte("contents"), fileData)
}
