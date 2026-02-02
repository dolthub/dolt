// Copyright 2024 Dolthub, Inc.
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

package dbfactory

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func createTestBareRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bareDir := filepath.Join(dir, "bare.git")

	_, err := git.PlainInit(bareDir, true)
	require.NoError(t, err)

	return bareDir
}

func TestIsGitURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{
			name:     "git scheme",
			url:      "git://github.com/user/repo.git",
			expected: true,
		},
		{
			name:     "https with .git suffix",
			url:      "https://github.com/user/repo.git",
			expected: true,
		},
		{
			name:     "http with .git suffix",
			url:      "http://github.com/user/repo.git",
			expected: true,
		},
		{
			name:     "https without .git suffix",
			url:      "https://github.com/user/repo",
			expected: false,
		},
		{
			name:     "dolthub remote",
			url:      "https://doltremoteapi.dolthub.com/user/repo",
			expected: false,
		},
		{
			name:     "file scheme",
			url:      "file:///path/to/repo",
			expected: false,
		},
		{
			name:     "git scheme uppercase",
			url:      "GIT://github.com/user/repo.git",
			expected: true,
		},
		{
			name:     "https with .GIT suffix uppercase",
			url:      "HTTPS://github.com/user/repo.GIT",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsGitURL(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGitRefFromParams(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]interface{}
		expected string
	}{
		{
			name:     "nil params",
			params:   nil,
			expected: "refs/dolt/data",
		},
		{
			name:     "empty params",
			params:   map[string]interface{}{},
			expected: "refs/dolt/data",
		},
		{
			name:     "custom ref",
			params:   map[string]interface{}{GitRefParam: "refs/custom/ref"},
			expected: "refs/custom/ref",
		},
		{
			name:     "empty ref string",
			params:   map[string]interface{}{GitRefParam: ""},
			expected: "refs/dolt/data",
		},
		{
			name:     "wrong type",
			params:   map[string]interface{}{GitRefParam: 123},
			expected: "refs/dolt/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gitRefFromParams(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGitLocalPathFromParams(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]interface{}
		expected string
	}{
		{
			name:     "nil params",
			params:   nil,
			expected: "",
		},
		{
			name:     "empty params",
			params:   map[string]interface{}{},
			expected: "",
		},
		{
			name:     "custom path",
			params:   map[string]interface{}{GitLocalPathParam: "/tmp/cache"},
			expected: "/tmp/cache",
		},
		{
			name:     "empty path string",
			params:   map[string]interface{}{GitLocalPathParam: ""},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gitLocalPathFromParams(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGitURLFromURLObj(t *testing.T) {
	tests := []struct {
		name     string
		urlStr   string
		expected string
	}{
		{
			name:     "git scheme converts to https",
			urlStr:   "git://github.com/user/repo.git",
			expected: "https://github.com/user/repo.git",
		},
		{
			name:     "https preserved",
			urlStr:   "https://github.com/user/repo.git",
			expected: "https://github.com/user/repo.git",
		},
		{
			name:     "http preserved",
			urlStr:   "http://github.com/user/repo.git",
			expected: "http://github.com/user/repo.git",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urlObj, err := url.Parse(tt.urlStr)
			require.NoError(t, err)
			result := gitURLFromURLObj(urlObj)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGitFactory_Registered(t *testing.T) {
	// Verify GitFactory is registered in DBFactories
	factory, ok := DBFactories[GitScheme]
	assert.True(t, ok, "GitFactory should be registered")
	_, isGitFactory := factory.(GitFactory)
	assert.True(t, isGitFactory, "should be GitFactory type")
}

func TestGitFactory_PrepareDB(t *testing.T) {
	bareDir := createTestBareRepo(t)
	ctx := context.Background()

	factory := GitFactory{}
	urlObj, err := url.Parse(bareDir)
	require.NoError(t, err)
	// For local file paths, we need to construct a proper file URL
	urlObj = &url.URL{
		Scheme: "git",
		Host:   "",
		Path:   bareDir,
	}

	nbf := types.Format_Default

	// PrepareDB should initialize the remote structure
	err = factory.PrepareDB(ctx, nbf, urlObj, nil)
	require.NoError(t, err)

	// Second call should be idempotent
	err = factory.PrepareDB(ctx, nbf, urlObj, nil)
	require.NoError(t, err)
}

func TestGitFactory_CreateDB(t *testing.T) {
	bareDir := createTestBareRepo(t)
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "work")

	factory := GitFactory{}
	urlObj := &url.URL{
		Scheme: "git",
		Host:   "",
		Path:   bareDir,
	}

	nbf := types.Format_Default
	params := map[string]interface{}{
		GitLocalPathParam: workDir,
	}

	// CreateDB should return a working database
	db, vrw, ns, err := factory.CreateDB(ctx, nbf, urlObj, params)
	require.NoError(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, vrw)
	assert.NotNil(t, ns)

	// Clean up
	err = db.Close()
	require.NoError(t, err)
}

func TestGitFactory_CreateDB_CustomRef(t *testing.T) {
	bareDir := createTestBareRepo(t)
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "work")

	factory := GitFactory{}
	urlObj := &url.URL{
		Scheme: "git",
		Host:   "",
		Path:   bareDir,
	}

	nbf := types.Format_Default
	params := map[string]interface{}{
		GitLocalPathParam: workDir,
		GitRefParam:       "refs/dolt/custom",
	}

	// CreateDB should work with custom ref
	db, vrw, ns, err := factory.CreateDB(ctx, nbf, urlObj, params)
	require.NoError(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, vrw)
	assert.NotNil(t, ns)

	// Clean up
	err = db.Close()
	require.NoError(t, err)
}
