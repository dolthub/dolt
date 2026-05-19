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

package errors

import (
	stderrors "errors"
	"fmt"
	"io"
	"syscall"
	"testing"
)

// TestFatalf_ENOSPCUnderCrashReturnsError documents the special-case
// behavior: under FatalBehaviorCrash, an ENOSPC argument causes Fatalf to
// return an error rather than crashing the process. Disk-exhaustion is
// recoverable by the operator and the panic-restart cycle amplifies the
// failure, so callers receive the error and can surface it cleanly.
func TestFatalf_ENOSPCUnderCrashReturnsError(t *testing.T) {
	err := Fatalf(FatalBehaviorCrash, "%w: error writing journal", syscall.ENOSPC)
	if err == nil {
		t.Fatal("expected returned error on ENOSPC under FatalBehaviorCrash, got nil")
	}
	if !stderrors.Is(err, syscall.ENOSPC) {
		t.Errorf("expected ENOSPC in error chain, got: %v", err)
	}
}

// TestFatalf_WrappedENOSPCUnderCrashReturnsError verifies that ENOSPC is
// detected even when it is wrapped inside another error (the common case
// when an OS write call produces a syscall error that the caller wraps
// with its own context before passing it to Fatalf).
func TestFatalf_WrappedENOSPCUnderCrashReturnsError(t *testing.T) {
	wrapped := fmt.Errorf("write /tmp/example: %w", syscall.ENOSPC)
	err := Fatalf(FatalBehaviorCrash, "%w: error during conjoin", wrapped)
	if err == nil {
		t.Fatal("expected returned error on wrapped ENOSPC, got nil")
	}
	if !stderrors.Is(err, syscall.ENOSPC) {
		t.Errorf("expected ENOSPC in error chain, got: %v", err)
	}
}

// TestFatalf_ErrorModeReturnsErrorAsBefore verifies that the existing
// FatalBehaviorError contract is unchanged: any error (ENOSPC or not) is
// returned wrapped.
func TestFatalf_ErrorModeReturnsErrorAsBefore(t *testing.T) {
	err := Fatalf(FatalBehaviorError, "%w: some other failure", io.EOF)
	if err == nil {
		t.Fatal("expected error in FatalBehaviorError mode")
	}
	if !stderrors.Is(err, io.EOF) {
		t.Errorf("expected EOF in error chain, got: %v", err)
	}
}

// TestAnyArgIsEnospc covers the helper directly.
func TestAnyArgIsEnospc(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []any
		want bool
	}{
		{name: "empty", args: nil, want: false},
		{name: "no_errors", args: []any{"not an error", 42}, want: false},
		{name: "unrelated_error", args: []any{io.EOF}, want: false},
		{name: "direct_enospc", args: []any{syscall.ENOSPC}, want: true},
		{name: "wrapped_enospc", args: []any{fmt.Errorf("ctx: %w", syscall.ENOSPC)}, want: true},
		{name: "second_arg_enospc", args: []any{"other", io.EOF, syscall.ENOSPC}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := anyArgIsEnospc(tc.args); got != tc.want {
				t.Errorf("anyArgIsEnospc(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
