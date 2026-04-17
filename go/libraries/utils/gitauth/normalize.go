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
	"bytes"
	"errors"
	"strings"
)

// NonInteractiveAuthError wraps a git remote error and appends credential
// hint lines so the user knows how to configure auth before retrying.
type NonInteractiveAuthError struct {
	Output string
	Cause  error
}

func (e *NonInteractiveAuthError) Error() string {
	var b strings.Builder
	if e.Output != "" {
		b.WriteString(strings.TrimRight(e.Output, "\n"))
		b.WriteString("\n")
	}
	b.WriteString("hint: dolt does not support interactive credential prompts\n")
	b.WriteString("hint: configure git credentials (credential helper, token) for HTTPS remotes\n")
	b.WriteString("hint: run `ssh-add <key>` to pre-load your key for SSH remotes\n")
	b.WriteString("hint: ensure non-interactive auth is configured for GCM")
	return b.String()
}

func (e *NonInteractiveAuthError) Unwrap() error { return e.Cause }

// NormalizeError wraps |err| in a [NonInteractiveAuthError] so that
// credential hints are always appended to git remote failures.
func NormalizeError(err error, output []byte) error {
	if err == nil {
		return nil
	}
	var already *NonInteractiveAuthError
	if errors.As(err, &already) {
		return err
	}
	return &NonInteractiveAuthError{
		Output: string(bytes.TrimSpace(output)),
		Cause:  err,
	}
}

var _ error = (*NonInteractiveAuthError)(nil)
