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
	b.WriteString("git authentication required but interactive prompting is disabled")
	b.WriteString("\n\nHints:")
	b.WriteString("\n- HTTPS: configure git credentials (credential helper, token) ahead of time")
	b.WriteString("\n- SSH: use ssh-agent / keychain and verify `ssh -o BatchMode=yes <host>` works")
	b.WriteString("\n- GCM: ensure non-interactive auth is configured")
	if strings.TrimSpace(e.Output) != "" {
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

// NormalizeError wraps err in a NonInteractiveAuthError when output and/or err indicate
// a credentials prompt or auth failure that would normally require prompting.
//
// output is optional; when empty, NormalizeError falls back to err.Error() for pattern matching.
func NormalizeError(err error, output []byte) error {
	if err == nil {
		return nil
	}
	var already *NonInteractiveAuthError
	if errors.As(err, &already) {
		return err
	}

	outStr := strings.TrimSpace(string(output))
	hay := outStr
	if hay == "" {
		hay = err.Error()
	} else {
		hay = hay + "\n" + err.Error()
	}
	if !looksLikeAuthPromptOrFailure(hay) {
		return err
	}
	if outStr == "" {
		outStr = strings.TrimSpace(err.Error())
	}
	return &NonInteractiveAuthError{Output: outStr, Cause: err}
}

func looksLikeAuthPromptOrFailure(s string) bool {
	// Keep these as simple substring matches; callers can extend as we observe new cases.
	s = strings.ToLower(s)
	patterns := []string{
		"terminal prompts disabled",
		"could not read Username",
		"could not read Password",
		"Authentication failed",
		"Enter passphrase for key",
		"Permission denied (publickey)",
		"fatal: could not read from remote repository",
	}
	for _, p := range patterns {
		if strings.Contains(s, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

var _ error = (*NonInteractiveAuthError)(nil)
