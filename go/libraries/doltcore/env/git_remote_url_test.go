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

package env

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeGitRemoteUrl(t *testing.T) {
	t.Run("empty not recognized", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("")
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("explicit git+https keeps scheme", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("git+https://example.com/org/repo.git")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "git+https://example.com/org/repo.git", got)
	})

	t.Run("explicit git+https with ref query is rejected", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("git+https://example.com/org/repo.git?ref=refs/dolt/data")
		require.Error(t, err)
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("https .git with ref query is rejected", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("https://example.com/org/repo.git?ref=refs/dolt/data")
		require.Error(t, err)
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("schemeless host/path with ref query is rejected", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("github.com/org/repo.git?ref=refs/dolt/data")
		require.Error(t, err)
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("https .git becomes git+https", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("https://example.com/org/repo.git")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "git+https://example.com/org/repo.git", got)
	})

	t.Run("scp-style becomes git+ssh", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("git@github.com:org/repo.git")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "git+ssh://git@github.com/org/repo.git", got)
	})

	t.Run("schemeless host/path defaults to git+https", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("github.com/org/repo.git")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "git+https://github.com/org/repo.git", got)
	})

	t.Run("local absolute path becomes git+file", func(t *testing.T) {
		p := filepath.ToSlash(filepath.Join(t.TempDir(), "remote.git"))
		got, ok, err := NormalizeGitRemoteUrl(p)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "git+file://"+p, got)
	})

	t.Run("non .git url not recognized", func(t *testing.T) {
		got, ok, err := NormalizeGitRemoteUrl("https://example.com/not-git")
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, got)
	})
}
