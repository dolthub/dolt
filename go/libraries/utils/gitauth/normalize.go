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

package gitauth

import (
	"errors"
	"strings"
)

// NonInteractiveAuthError indicates that a git operation failed because
// authentication was required but interactive prompting is disabled.
//
// It includes the captured git output (when available) to aid debugging.
type NonInteractiveAuthError struct {
	Output string
	Cause  error
}

func (e *NonInteractiveAuthError) Error() string {
	var b strings.Builder
	b.WriteString("remote authentication required but interactive prompting is disabled")
	b.WriteString("\n\nHints:")
	b.WriteString("\n- HTTPS: configure git credentials (credential helper, token) ahead of time")
	b.WriteString("\n- SSH: ensure your key is loaded (`ssh-add <key>`) and the server host key is in known_hosts")
	b.WriteString("\n- GCM: ensure non-interactive auth is configured")
	if e.Output != "" {
		b.WriteString("\n\nGit output:\n")
		b.WriteString(strings.TrimRight(e.Output, "\n"))
	}
	if e.Cause != nil {
		b.WriteString("\nOriginal error: ")
		b.WriteString(e.Cause.Error())
	}
	return b.String()
}

func (e *NonInteractiveAuthError) Unwrap() error { return e.Cause }

// NormalizeError wraps |err| in a [NonInteractiveAuthError] when |output|
// contains a git authentication prompt or failure message.
// When |output| does not match, |err| is returned unchanged.
func NormalizeError(err error, output []byte) error {
	if err == nil {
		return nil
	}
	var already *NonInteractiveAuthError
	if errors.As(err, &already) {
		return err
	}

	outStr := strings.TrimSpace(string(output))
	if !looksLikeAuthPromptOrFailure(outStr) {
		return err
	}
	return &NonInteractiveAuthError{Output: outStr, Cause: err}
}

func looksLikeAuthPromptOrFailure(s string) bool {
	// Keep these as simple substring matches; callers can extend as we observe new cases.
	s = strings.ToLower(s)
	for _, p := range []string{
		"terminal prompts disabled",
		"could not read username",
		"could not read password",
		"authentication failed",
		"enter passphrase for key",
		"permission denied (publickey)",
	} {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

var _ error = (*NonInteractiveAuthError)(nil)
