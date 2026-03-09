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

package dbfactory

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTransferCommand(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     string
		path     string
		user     string
		envSSH   string
		envExec  string
		wantArgs []string
		wantErr  string
	}{
		{
			name:     "basic host and path",
			host:     "example.com",
			path:     "/data/myrepo",
			wantArgs: []string{"ssh", "example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "with user",
			host:     "example.com",
			path:     "/data/myrepo",
			user:     "neil",
			wantArgs: []string{"ssh", "neil@example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "with port",
			host:     "example.com",
			port:     "2222",
			path:     "/data/myrepo",
			wantArgs: []string{"ssh", "-p", "2222", "example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "with user and port",
			host:     "example.com",
			port:     "2222",
			path:     "/data/myrepo",
			user:     "neil",
			wantArgs: []string{"ssh", "-p", "2222", "neil@example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "custom SSH command",
			host:     "example.com",
			path:     "/data/myrepo",
			envSSH:   "/usr/bin/my-ssh",
			wantArgs: []string{"/usr/bin/my-ssh", "example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "SSH command with arguments",
			host:     "example.com",
			path:     "/data/myrepo",
			envSSH:   "ssh -i /path/to/key -o StrictHostKeyChecking=no",
			wantArgs: []string{"ssh", "-i", "/path/to/key", "-o", "StrictHostKeyChecking=no", "example.com", "dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "custom remote dolt path",
			host:     "example.com",
			path:     "/data/myrepo",
			envExec:  "/usr/local/bin/dolt",
			wantArgs: []string{"ssh", "example.com", "/usr/local/bin/dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:     "custom SSH and remote dolt",
			host:     "example.com",
			path:     "/data/myrepo",
			user:     "ubuntu",
			envSSH:   "/path/to/wrapper.sh",
			envExec:  "/opt/dolt/bin/dolt",
			wantArgs: []string{"/path/to/wrapper.sh", "ubuntu@example.com", "/opt/dolt/bin/dolt --data-dir /data/myrepo transfer"},
		},
		{
			name:    "empty SSH command errors",
			host:    "example.com",
			path:    "/data/myrepo",
			envSSH:  "   ",
			wantErr: "invalid DOLT_SSH_COMMAND: empty",
		},
		{
			name:     "EC2 instance ID as host",
			host:     "i-0abc123def456",
			path:     "/home/ubuntu/mydb",
			envSSH:   "/path/to/ssm-wrapper.sh",
			wantArgs: []string{"/path/to/ssm-wrapper.sh", "i-0abc123def456", "dolt --data-dir /home/ubuntu/mydb transfer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSSH != "" {
				t.Setenv(EnvSSHCommand, tt.envSSH)
			}
			if tt.envExec != "" {
				t.Setenv(EnvSSHExecPath, tt.envExec)
			}

			cmd, err := buildTransferCommand(tt.host, tt.port, tt.path, tt.user)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)

			// cmd.Args includes argv[0] (the program) followed by the arguments.
			assert.Equal(t, tt.wantArgs, cmd.Args)
		})
	}
}

func TestFilterSSHNoise(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "only whitespace",
			input: "   \n\n   \n",
			want:  "",
		},
		{
			name:  "permanently added warning removed",
			input: "Warning: Permanently added '[127.0.0.1]:2222' (ED25519) to the list of known hosts.\nactual error\n",
			want:  "actual error",
		},
		{
			name:  "multiple warnings removed",
			input: "Warning: Permanently added 'host1' (RSA) to the list of known hosts.\nWarning: Permanently added 'host2' (ED25519) to the list of known hosts.\nreal error here\n",
			want:  "real error here",
		},
		{
			name:  "only warnings produces empty result",
			input: "Warning: Permanently added 'host' (ED25519) to the list of known hosts.\n",
			want:  "",
		},
		{
			name:  "real errors preserved",
			input: "failed to load database\ncause: missing dolt data directory\n",
			want:  "failed to load database\ncause: missing dolt data directory",
		},
		{
			name:  "mixed warnings and errors",
			input: "Warning: Permanently added 'host' (ED25519) to the list of known hosts.\nfailed to load database\n\ncause: missing dolt data directory\n",
			want:  "failed to load database\ncause: missing dolt data directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSSHNoise(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSSHRemoteError(t *testing.T) {
	tests := []struct {
		name      string
		stderr    string
		path      string
		msg       string
		err       error
		wantMsg   string
		wantExact bool
	}{
		{
			name:    "no stderr falls back to wrapped error",
			stderr:  "",
			path:    "/data/myrepo",
			msg:     "failed to create chunk store",
			err:     fmt.Errorf("connection refused"),
			wantMsg: "failed to create chunk store: connection refused",
		},
		{
			name:    "only SSH warnings falls back to wrapped error",
			stderr:  "Warning: Permanently added 'host' (ED25519) to the list of known hosts.\n",
			path:    "/data/myrepo",
			msg:     "failed to create chunk store",
			err:     fmt.Errorf("EOF"),
			wantMsg: "failed to create chunk store: EOF",
		},
		{
			name:    "missing dolt data directory gives repo not found",
			stderr:  "Warning: Permanently added 'host' (ED25519) to the list of known hosts.\nfailed to load database\ncause: missing dolt data directory\n",
			path:    "/data/myrepo",
			msg:     "failed to create SMUX client session",
			err:     fmt.Errorf("EOF"),
			wantMsg: "repository not found at /data/myrepo",
		},
		{
			name:    "no such file or directory gives repo not found",
			stderr:  "stat /nonexistent/path: no such file or directory\n",
			path:    "/nonexistent/path",
			msg:     "failed to create chunk store",
			err:     fmt.Errorf("EOF"),
			wantMsg: "repository not found at /nonexistent/path",
		},
		{
			name:    "other stderr error forwarded",
			stderr:  "dolt: command not found\n",
			path:    "/data/myrepo",
			msg:     "failed to create SMUX client session",
			err:     fmt.Errorf("EOF"),
			wantMsg: "failed to create SMUX client session: remote: dolt: command not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stderrDone := make(chan struct{})
			var stderrBuf bytes.Buffer
			stderrBuf.WriteString(tt.stderr)
			close(stderrDone)

			got := sshRemoteError(stderrDone, &stderrBuf, tt.path, tt.msg, tt.err)
			assert.Equal(t, tt.wantMsg, got.Error())
		})
	}
}

func TestSSHURLParsing(t *testing.T) {
	// Test the URL parsing logic from CreateDB by checking the arguments
	// that buildTransferCommand would receive for various SSH URLs.
	// This mirrors how CreateDB extracts host, port, path, and user.
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantPort string
		wantPath string
		wantUser string
	}{
		{
			name:     "simple host and path",
			url:      "ssh://myhost/data/repo",
			wantHost: "myhost",
			wantPath: "/data/repo",
		},
		{
			name:     "user in URL userinfo",
			url:      "ssh://neil@myhost/data/repo",
			wantHost: "myhost",
			wantPath: "/data/repo",
			wantUser: "neil",
		},
		{
			name:     "port in URL",
			url:      "ssh://myhost:2222/data/repo",
			wantHost: "myhost",
			wantPort: "2222",
			wantPath: "/data/repo",
		},
		{
			name:     "user and port",
			url:      "ssh://neil@myhost:2222/data/repo",
			wantHost: "myhost",
			wantPort: "2222",
			wantPath: "/data/repo",
			wantUser: "neil",
		},
		{
			name:     ".dolt suffix stripped",
			url:      "ssh://myhost/data/repo/.dolt",
			wantHost: "myhost",
			wantPath: "/data/repo",
		},
		{
			name:     "EC2 instance ID as host",
			url:      "ssh://i-0abc123def456/home/ubuntu/mydb",
			wantHost: "i-0abc123def456",
			wantPath: "/home/ubuntu/mydb",
		},
		{
			name:     "user with EC2 instance ID",
			url:      "ssh://ubuntu@i-0abc123def456/home/ubuntu/mydb",
			wantHost: "i-0abc123def456",
			wantPath: "/home/ubuntu/mydb",
			wantUser: "ubuntu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replicate the URL parsing from CreateDB.
			urlObj, err := parseSSHURL(tt.url)
			require.NoError(t, err)

			assert.Equal(t, tt.wantHost, urlObj.host, "host")
			assert.Equal(t, tt.wantPort, urlObj.port, "port")
			assert.Equal(t, tt.wantPath, urlObj.path, "path")
			assert.Equal(t, tt.wantUser, urlObj.user, "user")
		})
	}
}

// sshURLParts holds the parsed components of an SSH URL.
// This is a test helper that mirrors the parsing logic in CreateDB.
type sshURLParts struct {
	host string
	port string
	path string
	user string
}

// parseSSHURL replicates the URL parsing logic from SSHRemoteFactory.CreateDB.
func parseSSHURL(rawURL string) (sshURLParts, error) {
	urlObj, err := url.Parse(rawURL)
	if err != nil {
		return sshURLParts{}, err
	}

	host := urlObj.Hostname()
	port := urlObj.Port()
	path := urlObj.Path
	user := ""

	path = strings.TrimSuffix(path, "/.dolt")

	if urlObj.User != nil {
		user = urlObj.User.Username()
	}
	if atIdx := strings.LastIndex(host, "@"); atIdx != -1 {
		user = host[:atIdx]
		host = host[atIdx+1:]
	}

	return sshURLParts{host: host, port: port, path: path, user: user}, nil
}
