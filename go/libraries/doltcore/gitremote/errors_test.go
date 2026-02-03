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

package gitremote

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGitRemoteError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *GitRemoteError
		expected string
	}{
		{
			name: "basic error",
			err: &GitRemoteError{
				Op:  "clone",
				Err: errors.New("connection refused"),
			},
			expected: "git clone failed: connection refused",
		},
		{
			name: "error with URL",
			err: &GitRemoteError{
				Op:  "push",
				URL: "https://github.com/user/repo.git",
				Err: ErrPushRejected,
			},
			expected: "git push failed for https://github.com/user/repo.git: push rejected by remote",
		},
		{
			name: "error with URL and ref",
			err: &GitRemoteError{
				Op:  "fetch",
				URL: "https://github.com/user/repo.git",
				Ref: "refs/dolt/data",
				Err: ErrRefNotFound,
			},
			expected: "git fetch failed for https://github.com/user/repo.git (ref: refs/dolt/data): git ref not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestGitRemoteError_Hint(t *testing.T) {
	tests := []struct {
		name        string
		err         *GitRemoteError
		expectHint  bool
		hintContain string
	}{
		{
			name: "repo not initialized",
			err: &GitRemoteError{
				Op:  "push",
				Err: ErrRepoNotInitialized,
			},
			expectHint:  true,
			hintContain: "dolt remote init",
		},
		{
			name: "ref not found",
			err: &GitRemoteError{
				Op:  "fetch",
				Err: ErrRefNotFound,
			},
			expectHint:  true,
			hintContain: "dolt remote init",
		},
		{
			name: "push rejected",
			err: &GitRemoteError{
				Op:  "push",
				Err: ErrPushRejected,
			},
			expectHint:  true,
			hintContain: "Pull changes first",
		},
		{
			name: "auth failed",
			err: &GitRemoteError{
				Op:  "clone",
				Err: ErrAuthFailed,
			},
			expectHint:  true,
			hintContain: "credentials",
		},
		{
			name: "custom resolution",
			err: &GitRemoteError{
				Op:         "push",
				Err:        errors.New("some error"),
				Resolution: "Try again later",
			},
			expectHint:  true,
			hintContain: "Try again later",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint := tt.err.Hint()
			if tt.expectHint {
				assert.NotEmpty(t, hint)
				assert.Contains(t, hint, tt.hintContain)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name          string
		inputErr      error
		expectedError error
	}{
		{
			name:          "authentication error",
			inputErr:      errors.New("authentication required"),
			expectedError: ErrAuthFailed,
		},
		{
			name:          "permission denied",
			inputErr:      errors.New("Permission denied (publickey)"),
			expectedError: ErrAuthFailed,
		},
		{
			name:          "401 error",
			inputErr:      errors.New("server returned 401"),
			expectedError: ErrAuthFailed,
		},
		{
			name:          "repository not found",
			inputErr:      errors.New("repository not found"),
			expectedError: ErrRepoNotFound,
		},
		{
			name:          "404 error",
			inputErr:      errors.New("404 not found"),
			expectedError: ErrRepoNotFound,
		},
		{
			name:          "non-fast-forward",
			inputErr:      errors.New("non-fast-forward update"),
			expectedError: ErrPushRejected,
		},
		{
			name:          "push rejected",
			inputErr:      errors.New("push was rejected"),
			expectedError: ErrPushRejected,
		},
		{
			name:          "network timeout",
			inputErr:      errors.New("connection timeout"),
			expectedError: ErrNetworkError,
		},
		{
			name:          "no such host",
			inputErr:      errors.New("dial tcp: no such host"),
			expectedError: ErrNetworkError,
		},
		{
			name:          "reference not found",
			inputErr:      errors.New("reference not found"),
			expectedError: ErrRefNotFound,
		},
		{
			name:          "unknown error",
			inputErr:      errors.New("something unexpected"),
			expectedError: nil, // Should return original error
		},
		{
			name:          "nil error",
			inputErr:      nil,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyError(tt.inputErr)
			if tt.expectedError != nil {
				assert.True(t, errors.Is(result, tt.expectedError),
					"expected error to wrap %v, got %v", tt.expectedError, result)
			} else if tt.inputErr != nil {
				// Unknown errors should be returned as-is
				assert.Equal(t, tt.inputErr, result)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestErrorHelpers(t *testing.T) {
	t.Run("IsAuthError", func(t *testing.T) {
		assert.True(t, IsAuthError(ErrAuthFailed))
		assert.True(t, IsAuthError(classifyError(errors.New("authentication required"))))
		assert.False(t, IsAuthError(ErrPushRejected))
	})

	t.Run("IsPushRejectedError", func(t *testing.T) {
		assert.True(t, IsPushRejectedError(ErrPushRejected))
		assert.True(t, IsPushRejectedError(classifyError(errors.New("non-fast-forward"))))
		assert.False(t, IsPushRejectedError(ErrAuthFailed))
	})

	t.Run("IsNotFoundError", func(t *testing.T) {
		assert.True(t, IsNotFoundError(ErrRepoNotFound))
		assert.True(t, IsNotFoundError(ErrRefNotFound))
		assert.False(t, IsNotFoundError(ErrAuthFailed))
	})

	t.Run("IsNetworkError", func(t *testing.T) {
		assert.True(t, IsNetworkError(ErrNetworkError))
		assert.True(t, IsNetworkError(classifyError(errors.New("connection timeout"))))
		assert.False(t, IsNetworkError(ErrAuthFailed))
	})
}

func TestFormatErrorWithHint(t *testing.T) {
	t.Run("GitRemoteError with hint", func(t *testing.T) {
		err := &GitRemoteError{
			Op:  "push",
			URL: "https://github.com/user/repo.git",
			Err: ErrPushRejected,
		}
		formatted := FormatErrorWithHint(err)
		assert.Contains(t, formatted, "git push failed")
		assert.Contains(t, formatted, "hint:")
	})

	t.Run("regular error", func(t *testing.T) {
		err := errors.New("some error")
		formatted := FormatErrorWithHint(err)
		assert.Equal(t, "some error", formatted)
	})
}

func TestNewErrorFunctions(t *testing.T) {
	t.Run("NewCloneError", func(t *testing.T) {
		err := NewCloneError("https://github.com/user/repo.git", errors.New("connection refused"))
		assert.Equal(t, "clone", err.Op)
		assert.Equal(t, "https://github.com/user/repo.git", err.URL)
	})

	t.Run("NewPushError", func(t *testing.T) {
		err := NewPushError("https://github.com/user/repo.git", "refs/dolt/data", errors.New("rejected"))
		assert.Equal(t, "push", err.Op)
		assert.Equal(t, "refs/dolt/data", err.Ref)
		assert.True(t, errors.Is(err.Err, ErrPushRejected))
	})

	t.Run("NewFetchError", func(t *testing.T) {
		err := NewFetchError("https://github.com/user/repo.git", "refs/dolt/data", errors.New("not found"))
		assert.Equal(t, "fetch", err.Op)
	})

	t.Run("NewAuthError", func(t *testing.T) {
		err := NewAuthError("https://github.com/user/repo.git", errors.New("invalid credentials"))
		assert.Equal(t, "authenticate", err.Op)
		assert.True(t, errors.Is(err.Err, ErrAuthFailed))
	})
}

func TestGitRemoteError_Unwrap(t *testing.T) {
	underlying := errors.New("underlying error")
	err := &GitRemoteError{
		Op:  "push",
		Err: underlying,
	}

	assert.Equal(t, underlying, errors.Unwrap(err))
	assert.True(t, errors.Is(err, underlying))
}
