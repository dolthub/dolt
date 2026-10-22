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

package file

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameNoReplace(t *testing.T) {
	t.Run("NewPathDoesNotExist", func(t *testing.T) {
		dir := t.TempDir()
		oldpath, newpath := filepath.Join(dir, "old"), filepath.Join(dir, "new")
		require.NoError(t, os.WriteFile(oldpath, []byte("old"), 0644))

		require.NoError(t, RenameNoReplace(oldpath, newpath))

		_, err := os.Stat(oldpath)
		assert.ErrorIs(t, err, fs.ErrNotExist)
		got, err := os.ReadFile(newpath)
		require.NoError(t, err)
		assert.Equal(t, "old", string(got))
	})
	t.Run("NewPathExists", func(t *testing.T) {
		dir := t.TempDir()
		oldpath, newpath := filepath.Join(dir, "old"), filepath.Join(dir, "new")
		require.NoError(t, os.WriteFile(oldpath, []byte("old"), 0644))
		require.NoError(t, os.WriteFile(newpath, []byte("new"), 0644))

		err := RenameNoReplace(oldpath, newpath)
		assert.ErrorIs(t, err, fs.ErrExist)

		got, err := os.ReadFile(oldpath)
		require.NoError(t, err)
		assert.Equal(t, "old", string(got))
		got, err = os.ReadFile(newpath)
		require.NoError(t, err)
		assert.Equal(t, "new", string(got))
	})
	t.Run("LinkFallback", func(t *testing.T) {
		dir := t.TempDir()
		oldpath, newpath := filepath.Join(dir, "old"), filepath.Join(dir, "new")
		require.NoError(t, os.WriteFile(oldpath, []byte("old"), 0644))
		require.NoError(t, os.WriteFile(newpath, []byte("new"), 0644))

		assert.ErrorIs(t, linkNoReplace(oldpath, newpath), fs.ErrExist)
		require.NoError(t, os.Remove(newpath))
		require.NoError(t, linkNoReplace(oldpath, newpath))
		_, err := os.Stat(oldpath)
		assert.ErrorIs(t, err, fs.ErrNotExist)
		got, err := os.ReadFile(newpath)
		require.NoError(t, err)
		assert.Equal(t, "old", string(got))
	})
}

func TestTouch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, []byte("contents"), 0644))
	aged := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(p, aged, aged))

	require.NoError(t, Touch(p))

	info, err := os.Stat(p)
	require.NoError(t, err)
	assert.True(t, info.ModTime().After(aged.Add(30*time.Minute)))
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.Equal(t, "contents", string(got))

	assert.ErrorIs(t, Touch(filepath.Join(t.TempDir(), "missing")), fs.ErrNotExist)
}
