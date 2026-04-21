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
	"testing"
)

func TestNormalizeError_AlwaysWrapsAndAppendsHints(t *testing.T) {
	cases := []struct {
		name   string
		output string
	}{
		{"terminal prompts disabled", "fatal: could not read Username for 'https://example.com': terminal prompts disabled"},
		{"permission denied publickey", "Permission denied (publickey)."},
		{"permission denied keyboard-interactive", "Permission denied (publickey,keyboard-interactive)."},
		{"enter passphrase", "Enter passphrase for key '/tmp/fake_key': "},
		{"connection closed", "Connection closed by UNKNOWN port 65535\nfatal: Could not read from remote repository."},
		{"empty output", ""},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeError(errors.New("git failed"), []byte(tt.output))

			var niae *NonInteractiveAuthError
			if !errors.As(got, &niae) {
				t.Fatalf("expected NonInteractiveAuthError, got %T: %v", got, got)
			}
			msg := got.Error()
			if !strings.Contains(msg, "hint:") {
				t.Fatalf("expected hint lines, got: %q", msg)
			}
			if tt.output != "" && !strings.Contains(msg, strings.TrimSpace(tt.output)) {
				t.Fatalf("expected output preserved, got: %q", msg)
			}
		})
	}
}

func TestNormalizeError_Idempotent(t *testing.T) {
	base := errors.New("git failed")
	authOutput := []byte("Permission denied (publickey).")
	got1 := NormalizeError(base, authOutput)
	got2 := NormalizeError(got1, authOutput)
	if got1 != got2 {
		t.Fatalf("expected NormalizeError to be idempotent when already normalized")
	}
}
