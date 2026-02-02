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
	"errors"
	"fmt"
	"strings"
)

// Git remote error types with user-friendly messages and resolution hints.
var (
	// ErrRepoNotInitialized indicates the git repository hasn't been set up for dolt.
	ErrRepoNotInitialized = errors.New("git repository not initialized for dolt remote")

	// ErrRefNotFound indicates the custom dolt ref doesn't exist.
	ErrRefNotFound = errors.New("git ref not found")

	// ErrNothingToCommit indicates there are no changes to commit.
	ErrNothingToCommit = errors.New("nothing to commit")

	// ErrPushRejected indicates the push was rejected by the remote.
	ErrPushRejected = errors.New("push rejected by remote")

	// ErrAuthFailed indicates git authentication failed.
	ErrAuthFailed = errors.New("git authentication failed")

	// ErrRepoNotFound indicates the git repository doesn't exist.
	ErrRepoNotFound = errors.New("git repository not found")

	// ErrNetworkError indicates a network connectivity issue.
	ErrNetworkError = errors.New("network error connecting to git remote")

	// ErrInvalidURL indicates the git URL is malformed.
	ErrInvalidURL = errors.New("invalid git repository URL")

	// ErrPermissionDenied indicates insufficient permissions.
	ErrPermissionDenied = errors.New("permission denied")
)

// GitRemoteError wraps an error with additional context for git remote operations.
type GitRemoteError struct {
	Op         string // Operation that failed (e.g., "clone", "push", "fetch")
	URL        string // Git repository URL
	Ref        string // Git ref being operated on
	Err        error  // Underlying error
	Resolution string // Suggested resolution
}

func (e *GitRemoteError) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("git %s failed", e.Op))
	if e.URL != "" {
		sb.WriteString(fmt.Sprintf(" for %s", e.URL))
	}
	if e.Ref != "" {
		sb.WriteString(fmt.Sprintf(" (ref: %s)", e.Ref))
	}
	if e.Err != nil {
		sb.WriteString(fmt.Sprintf(": %v", e.Err))
	}
	return sb.String()
}

func (e *GitRemoteError) Unwrap() error {
	return e.Err
}

// Hint returns a user-friendly hint for resolving the error.
func (e *GitRemoteError) Hint() string {
	if e.Resolution != "" {
		return e.Resolution
	}

	// Provide default hints based on error type
	if errors.Is(e.Err, ErrRepoNotInitialized) {
		return "Run 'dolt remote init <git-url>' to initialize the git repository for dolt."
	}
	if errors.Is(e.Err, ErrRefNotFound) {
		return "The dolt data ref doesn't exist. Run 'dolt remote init <git-url>' first."
	}
	if errors.Is(e.Err, ErrPushRejected) {
		return "The remote has changes you don't have locally. Pull changes first or use --force."
	}
	if errors.Is(e.Err, ErrAuthFailed) {
		return "Check your git credentials. For SSH, ensure your key is available (ssh-agent / ~/.ssh config). For HTTPS, ensure your git credential helper / keychain / ~/.netrc is configured."
	}
	if errors.Is(e.Err, ErrRepoNotFound) {
		return "Verify the git repository URL is correct and the repository exists."
	}
	if errors.Is(e.Err, ErrNetworkError) {
		return "Check your network connection and verify the git remote is accessible."
	}
	if errors.Is(e.Err, ErrInvalidURL) {
		return "Git remote URLs must use the git:// scheme or end with .git (e.g., https://github.com/user/repo.git)."
	}
	if errors.Is(e.Err, ErrPermissionDenied) {
		return "You don't have permission to access this repository. Check your credentials and repository access rights."
	}

	return ""
}

// NewCloneError creates an error for clone operations.
func NewCloneError(url string, err error) *GitRemoteError {
	return &GitRemoteError{
		Op:  "clone",
		URL: url,
		Err: classifyError(err),
	}
}

// NewPushError creates an error for push operations.
func NewPushError(url, ref string, err error) *GitRemoteError {
	return &GitRemoteError{
		Op:  "push",
		URL: url,
		Ref: ref,
		Err: classifyError(err),
	}
}

// NewFetchError creates an error for fetch operations.
func NewFetchError(url, ref string, err error) *GitRemoteError {
	return &GitRemoteError{
		Op:  "fetch",
		URL: url,
		Ref: ref,
		Err: classifyError(err),
	}
}

// NewAuthError creates an error for authentication failures.
func NewAuthError(url string, err error) *GitRemoteError {
	return &GitRemoteError{
		Op:  "authenticate",
		URL: url,
		Err: ErrAuthFailed,
	}
}

// classifyError attempts to classify a generic error into a known error type.
func classifyError(err error) error {
	if err == nil {
		return nil
	}

	errStr := strings.ToLower(err.Error())

	// Authentication errors
	if strings.Contains(errStr, "authentication") ||
		strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "publickey") ||
		strings.Contains(errStr, "invalid credentials") ||
		strings.Contains(errStr, "401") ||
		strings.Contains(errStr, "403") {
		return fmt.Errorf("%w: %v", ErrAuthFailed, err)
	}

	// Repository not found
	if strings.Contains(errStr, "repository not found") ||
		strings.Contains(errStr, "not found") && strings.Contains(errStr, "repo") ||
		strings.Contains(errStr, "404") {
		return fmt.Errorf("%w: %v", ErrRepoNotFound, err)
	}

	// Push rejected
	if strings.Contains(errStr, "non-fast-forward") ||
		strings.Contains(errStr, "rejected") ||
		strings.Contains(errStr, "failed to push") {
		return fmt.Errorf("%w: %v", ErrPushRejected, err)
	}

	// Network errors
	if strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "dial") ||
		strings.Contains(errStr, "no such host") {
		return fmt.Errorf("%w: %v", ErrNetworkError, err)
	}

	// Reference not found
	if strings.Contains(errStr, "reference not found") ||
		strings.Contains(errStr, "couldn't find remote ref") {
		return fmt.Errorf("%w: %v", ErrRefNotFound, err)
	}

	// Permission denied
	if strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "access denied") {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}

	return err
}

// IsAuthError returns true if the error is an authentication error.
func IsAuthError(err error) bool {
	return errors.Is(err, ErrAuthFailed)
}

// IsPushRejectedError returns true if the error is a push rejection.
func IsPushRejectedError(err error) bool {
	return errors.Is(err, ErrPushRejected)
}

// IsNotFoundError returns true if the error indicates something wasn't found.
func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrRepoNotFound) || errors.Is(err, ErrRefNotFound)
}

// IsNetworkError returns true if the error is a network-related error.
func IsNetworkError(err error) bool {
	return errors.Is(err, ErrNetworkError)
}

// FormatErrorWithHint formats an error with its resolution hint for display.
func FormatErrorWithHint(err error) string {
	var gitErr *GitRemoteError
	if errors.As(err, &gitErr) {
		hint := gitErr.Hint()
		if hint != "" {
			return fmt.Sprintf("%s\nhint: %s", gitErr.Error(), hint)
		}
		return gitErr.Error()
	}
	return err.Error()
}
