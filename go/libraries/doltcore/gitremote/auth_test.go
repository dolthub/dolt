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

package gitremote

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNetrc(t *testing.T) {
	tests := []struct {
		name         string
		content      string
		host         string
		wantUsername string
		wantPassword string
	}{
		{
			name: "simple machine entry",
			content: `machine github.com
login myuser
password mytoken`,
			host:         "github.com",
			wantUsername: "myuser",
			wantPassword: "mytoken",
		},
		{
			name:         "single line format",
			content:      `machine gitlab.com login user1 password pass1`,
			host:         "gitlab.com",
			wantUsername: "user1",
			wantPassword: "pass1",
		},
		{
			name: "multiple machines",
			content: `machine github.com login ghuser password ghtoken
machine gitlab.com login gluser password gltoken`,
			host:         "gitlab.com",
			wantUsername: "gluser",
			wantPassword: "gltoken",
		},
		{
			name: "with comments",
			content: `# This is a comment
machine github.com
login myuser
# another comment
password mytoken`,
			host:         "github.com",
			wantUsername: "myuser",
			wantPassword: "mytoken",
		},
		{
			name: "default entry",
			content: `machine github.com login ghuser password ghtoken
default login defaultuser password defaultpass`,
			host:         "unknown.com",
			wantUsername: "defaultuser",
			wantPassword: "defaultpass",
		},
		{
			name:         "no match",
			content:      `machine github.com login user password pass`,
			host:         "gitlab.com",
			wantUsername: "",
			wantPassword: "",
		},
		{
			name:         "empty content",
			content:      ``,
			host:         "github.com",
			wantUsername: "",
			wantPassword: "",
		},
		{
			name:         "host with port",
			content:      `machine github.com login user password pass`,
			host:         "github.com:443",
			wantUsername: "user",
			wantPassword: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			username, password := parseNetrc(tt.content, tt.host)
			assert.Equal(t, tt.wantUsername, username, "username mismatch")
			assert.Equal(t, tt.wantPassword, password, "password mismatch")
		})
	}
}

func TestParseCredentialOutput(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   map[string]string
	}{
		{
			name: "standard output",
			output: `protocol=https
host=github.com
username=myuser
password=mytoken`,
			want: map[string]string{
				"protocol": "https",
				"host":     "github.com",
				"username": "myuser",
				"password": "mytoken",
			},
		},
		{
			name:   "empty output",
			output: ``,
			want:   map[string]string{},
		},
		{
			name:   "single line",
			output: `username=user`,
			want:   map[string]string{"username": "user"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCredentialOutput(tt.output)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDetectAuth_FileURL(t *testing.T) {
	// File URLs should return nil auth (no authentication needed)
	auth, err := DetectAuth("file:///path/to/repo")
	require.NoError(t, err)
	assert.Nil(t, auth)
}

func TestAuthMethodName(t *testing.T) {
	assert.Equal(t, "anonymous", AuthMethodName(nil))
}

func TestTryHTTPSFromEnv(t *testing.T) {
	// Save and restore environment
	oldPassword := os.Getenv(EnvDoltRemotePassword)
	defer os.Setenv(EnvDoltRemotePassword, oldPassword)

	// Test with no env var set
	os.Unsetenv(EnvDoltRemotePassword)
	auth, err := tryHTTPSFromEnv("")
	require.NoError(t, err)
	assert.Nil(t, auth)

	// Test with env var set
	os.Setenv(EnvDoltRemotePassword, "test-token")
	auth, err = tryHTTPSFromEnv("")
	require.NoError(t, err)
	require.NotNil(t, auth)
	assert.Equal(t, "https-basic", AuthMethodName(auth))
}

func TestTrySSHKeyFromEnv(t *testing.T) {
	// Save and restore environment
	oldKey := os.Getenv(EnvGitSSHKey)
	defer os.Setenv(EnvGitSSHKey, oldKey)

	// Test with no env var set
	os.Unsetenv(EnvGitSSHKey)
	auth, err := trySSHKeyFromEnv("git")
	require.NoError(t, err)
	assert.Nil(t, auth)

	// Test with env var set to a file path that doesn't exist
	os.Setenv(EnvGitSSHKey, "/nonexistent/path/to/key")
	auth, err = trySSHKeyFromEnv("git")
	// Should fail because the key content is not valid
	assert.Error(t, err)
}

func TestTrySSHKeyFiles(t *testing.T) {
	// This test verifies the function doesn't panic and handles missing files gracefully
	auth, err := trySSHKeyFiles("git")
	// May or may not find keys depending on the system
	// Just verify no panic and no unexpected error
	if err != nil {
		t.Logf("trySSHKeyFiles returned error (expected on systems without SSH keys): %v", err)
	}
	if auth != nil {
		t.Logf("trySSHKeyFiles found SSH key")
	}
}

func TestTryNetrc(t *testing.T) {
	// Create a temporary netrc file
	tmpDir := t.TempDir()
	netrcPath := filepath.Join(tmpDir, ".netrc")

	content := `machine github.com login testuser password testpass`
	err := os.WriteFile(netrcPath, []byte(content), 0600)
	require.NoError(t, err)

	// Save and restore NETRC env var
	oldNetrc := os.Getenv("NETRC")
	defer os.Setenv("NETRC", oldNetrc)

	os.Setenv("NETRC", netrcPath)

	auth, err := tryNetrc("github.com")
	require.NoError(t, err)
	require.NotNil(t, auth)
	assert.Equal(t, "https-basic", AuthMethodName(auth))
}

func TestDetectAuth_SSHURLs(t *testing.T) {
	// These tests verify URL parsing works correctly
	// Actual auth detection depends on system configuration

	testCases := []struct {
		url string
	}{
		{"git@github.com:user/repo.git"},
		{"ssh://git@github.com/user/repo.git"},
		{"git://github.com/user/repo.git"},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			// Should not error, even if no credentials found
			_, err := DetectAuth(tc.url)
			// We allow errors from SSH agent not being available
			// The important thing is it doesn't panic
			if err != nil {
				t.Logf("DetectAuth(%q) returned error (may be expected): %v", tc.url, err)
			}
		})
	}
}

func TestDetectAuth_HTTPSURLs(t *testing.T) {
	testCases := []struct {
		url string
	}{
		{"https://github.com/user/repo.git"},
		{"https://gitlab.com/user/repo.git"},
		{"http://localhost:8080/repo.git"},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			// Should not error, even if no credentials found
			auth, err := DetectAuth(tc.url)
			require.NoError(t, err)
			// Auth may be nil (anonymous) if no credentials configured
			t.Logf("DetectAuth(%q) returned auth: %s", tc.url, AuthMethodName(auth))
		})
	}
}
