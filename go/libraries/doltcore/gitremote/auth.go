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
	"bufio"
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
)

const (
	// DefaultSSHUser is the default username for SSH git operations
	DefaultSSHUser = "git"

	// Environment variable names
	EnvGitSSHKey          = "GIT_SSH_KEY"
	EnvDoltRemotePassword = "DOLT_REMOTE_PASSWORD"
)

// Common SSH key file names in order of preference
var defaultSSHKeyFiles = []string{
	"id_ed25519",
	"id_ecdsa",
	"id_rsa",
	"id_dsa",
}

// DetectAuth automatically detects and returns appropriate authentication
// for the given git repository URL. It tries multiple credential sources
// in order of preference and returns the first successful one.
//
// For SSH URLs (git@host:path or ssh://), it tries:
//  1. SSH agent
//  2. SSH key files from default locations
//  3. SSH key from GIT_SSH_KEY environment variable
//
// For HTTPS URLs, it tries:
//  1. Git credential helper
//  2. Environment variables (DOLT_REMOTE_PASSWORD)
//  3. Netrc file
//
// Returns nil auth (anonymous) if no credentials are found, which may
// still work for public repositories.
func DetectAuth(repoURL string) (transport.AuthMethod, error) {
	endpoint, err := transport.NewEndpoint(repoURL)
	if err != nil {
		return nil, fmt.Errorf("invalid git URL %q: %w", repoURL, err)
	}

	switch endpoint.Protocol {
	case "ssh", "git":
		return DetectSSHAuth(endpoint.User)
	case "https", "http":
		return DetectHTTPSAuth(endpoint.Host, endpoint.User)
	case "file":
		// Local file URLs don't need authentication
		return nil, nil
	default:
		// For unknown protocols, try HTTPS auth as fallback
		return DetectHTTPSAuth(endpoint.Host, endpoint.User)
	}
}

// DetectSSHAuth attempts to find SSH credentials for git operations.
// It tries sources in the following order:
//  1. SSH agent (if running and has keys)
//  2. SSH key files from ~/.ssh/
//  3. SSH key from GIT_SSH_KEY environment variable
//
// The user parameter specifies the SSH username (defaults to "git").
func DetectSSHAuth(user string) (transport.AuthMethod, error) {
	if user == "" {
		user = DefaultSSHUser
	}

	// Try SSH agent first
	auth, err := trySSHAgent(user)
	if err == nil && auth != nil {
		return auth, nil
	}

	// Try default SSH key files
	auth, err = trySSHKeyFiles(user)
	if err == nil && auth != nil {
		return auth, nil
	}

	// Try GIT_SSH_KEY environment variable
	auth, err = trySSHKeyFromEnv(user)
	if err == nil && auth != nil {
		return auth, nil
	}

	// No SSH credentials found - return nil (anonymous)
	// This allows public repos to work without credentials
	return nil, nil
}

// DetectHTTPSAuth attempts to find HTTPS credentials for git operations.
// It tries sources in the following order:
//  1. Git credential helper
//  2. DOLT_REMOTE_PASSWORD environment variable
//  3. Netrc file
//
// The host parameter is the git server hostname.
// The user parameter is optional; if empty, it may be filled by credential sources.
func DetectHTTPSAuth(host, user string) (transport.AuthMethod, error) {
	// Try git credential helper first
	auth, err := tryGitCredentialHelper(host, user)
	if err == nil && auth != nil {
		return auth, nil
	}

	// Try environment variable
	auth, err = tryHTTPSFromEnv(user)
	if err == nil && auth != nil {
		return auth, nil
	}

	// Try netrc
	auth, err = tryNetrc(host)
	if err == nil && auth != nil {
		return auth, nil
	}

	// No HTTPS credentials found - return nil (anonymous)
	return nil, nil
}

// trySSHAgent attempts to use the SSH agent for authentication.
func trySSHAgent(user string) (transport.AuthMethod, error) {
	auth, err := ssh.NewSSHAgentAuth(user)
	if err != nil {
		return nil, err
	}
	return auth, nil
}

// trySSHKeyFiles looks for SSH key files in the default ~/.ssh/ directory.
func trySSHKeyFiles(user string) (transport.AuthMethod, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	sshDir := filepath.Join(homeDir, ".ssh")

	for _, keyName := range defaultSSHKeyFiles {
		keyPath := filepath.Join(sshDir, keyName)
		if _, err := os.Stat(keyPath); err == nil {
			auth, err := ssh.NewPublicKeysFromFile(user, keyPath, "")
			if err == nil {
				return auth, nil
			}
			// Key exists but couldn't be loaded (maybe encrypted)
			// Try next key
		}
	}

	return nil, nil
}

// trySSHKeyFromEnv tries to load an SSH key from the GIT_SSH_KEY environment variable.
// The variable can contain either a file path or the key content directly.
func trySSHKeyFromEnv(user string) (transport.AuthMethod, error) {
	keyValue := os.Getenv(EnvGitSSHKey)
	if keyValue == "" {
		return nil, nil
	}

	// Check if it's a file path
	if _, err := os.Stat(keyValue); err == nil {
		return ssh.NewPublicKeysFromFile(user, keyValue, "")
	}

	// Treat as key content
	return ssh.NewPublicKeys(user, []byte(keyValue), "")
}

// tryGitCredentialHelper runs the git credential helper to get credentials.
func tryGitCredentialHelper(host, user string) (transport.AuthMethod, error) {
	// Check if git is available
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return nil, nil // git not available, skip
	}

	// Prepare credential request
	input := fmt.Sprintf("protocol=https\nhost=%s\n", host)
	if user != "" {
		input += fmt.Sprintf("username=%s\n", user)
	}
	input += "\n"

	cmd := exec.Command(gitPath, "credential", "fill")
	cmd.Stdin = strings.NewReader(input)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return nil, nil // credential helper failed, skip
	}

	// Parse output
	creds := parseCredentialOutput(stdout.String())
	username := creds["username"]
	password := creds["password"]

	if username == "" || password == "" {
		return nil, nil
	}

	return &http.BasicAuth{
		Username: username,
		Password: password,
	}, nil
}

// parseCredentialOutput parses the output from git credential fill.
func parseCredentialOutput(output string) map[string]string {
	result := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "="); idx > 0 {
			key := line[:idx]
			value := line[idx+1:]
			result[key] = value
		}
	}
	return result
}

// tryHTTPSFromEnv tries to get HTTPS credentials from environment variables.
func tryHTTPSFromEnv(user string) (transport.AuthMethod, error) {
	password := os.Getenv(EnvDoltRemotePassword)
	if password == "" {
		return nil, nil
	}

	// If no user specified, try common defaults
	if user == "" {
		// For token-based auth (GitHub, GitLab), username can be anything
		user = "x-access-token"
	}

	return &http.BasicAuth{
		Username: user,
		Password: password,
	}, nil
}

// tryNetrc attempts to find credentials in the netrc file.
func tryNetrc(host string) (transport.AuthMethod, error) {
	netrcPath := getNetrcPath()
	if netrcPath == "" {
		return nil, nil
	}

	data, err := os.ReadFile(netrcPath)
	if err != nil {
		return nil, nil
	}

	username, password := parseNetrc(string(data), host)
	if username == "" || password == "" {
		return nil, nil
	}

	return &http.BasicAuth{
		Username: username,
		Password: password,
	}, nil
}

// getNetrcPath returns the path to the netrc file.
func getNetrcPath() string {
	// Check NETRC environment variable first
	if netrc := os.Getenv("NETRC"); netrc != "" {
		return netrc
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	// On Windows, it's _netrc; on Unix, it's .netrc
	if runtime.GOOS == "windows" {
		return filepath.Join(homeDir, "_netrc")
	}
	return filepath.Join(homeDir, ".netrc")
}

// parseNetrc parses a netrc file and returns credentials for the given host.
// This is a simplified parser that handles the common format:
//
//	machine <host> login <user> password <pass>
func parseNetrc(content, host string) (username, password string) {
	// Normalize host (remove port if present)
	if u, err := url.Parse("https://" + host); err == nil {
		host = u.Hostname()
	}

	lines := strings.Split(content, "\n")
	var currentMachine string
	var currentLogin, currentPassword string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		for i := 0; i < len(fields); i++ {
			switch fields[i] {
			case "machine":
				// Save previous machine's credentials if it matches
				if currentMachine == host && currentLogin != "" && currentPassword != "" {
					return currentLogin, currentPassword
				}
				currentMachine = ""
				currentLogin = ""
				currentPassword = ""
				if i+1 < len(fields) {
					currentMachine = fields[i+1]
					i++
				}
			case "default":
				// Save previous machine's credentials if it matches
				if currentMachine == host && currentLogin != "" && currentPassword != "" {
					return currentLogin, currentPassword
				}
				currentMachine = "*" // wildcard
				currentLogin = ""
				currentPassword = ""
			case "login":
				if i+1 < len(fields) {
					currentLogin = fields[i+1]
					i++
				}
			case "password":
				if i+1 < len(fields) {
					currentPassword = fields[i+1]
					i++
				}
			}
		}
	}

	// Check last machine
	if currentMachine == host || currentMachine == "*" {
		if currentLogin != "" && currentPassword != "" {
			return currentLogin, currentPassword
		}
	}

	return "", ""
}

// AuthMethodName returns a human-readable name for the auth method type.
// Useful for logging and debugging.
func AuthMethodName(auth transport.AuthMethod) string {
	if auth == nil {
		return "anonymous"
	}

	switch auth.(type) {
	case *ssh.PublicKeysCallback:
		return "ssh-agent"
	case *ssh.PublicKeys:
		return "ssh-key"
	case *http.BasicAuth:
		return "https-basic"
	case *http.TokenAuth:
		return "https-token"
	default:
		return fmt.Sprintf("%T", auth)
	}
}
