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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultRef is the default git ref used for dolt remote data
	DefaultRef = "refs/dolt/data"

	// DefaultRemoteName is the default name for the git remote
	DefaultRemoteName = "origin"

	// DoltRemoteDir is the directory within the git tree that contains dolt data
	DoltRemoteDir = ".dolt_remote"

	// DoltRemoteDataDir is the subdirectory for actual data files
	DoltRemoteDataDir = ".dolt_remote/data"
)

// Note: Error types are defined in errors.go

// GitRepo provides a high-level interface for git operations on a dolt remote.
// It shells out to the system `git` CLI and handles custom ref operations for storing dolt data.
type GitRepo struct {
	url         string
	ref         string
	localPath   string
	ownsWorkdir bool
	mu          sync.RWMutex
}

var (
	// ErrDirtyWorktree is returned when an operation would need to clobber existing
	// uncommitted changes in the underlying git worktree in order to proceed.
	ErrDirtyWorktree = errors.New("git worktree has uncommitted changes; refusing forced checkout")

	// ErrInvalidPath is returned when a requested file path escapes the repo root.
	ErrInvalidPath = errors.New("invalid path")
)

func validateNoCtl(name, s string) error {
	if strings.ContainsAny(s, "\x00\r\n") {
		return fmt.Errorf("%s contains invalid control characters", name)
	}
	return nil
}

func validateRefName(ctx context.Context, ref string) error {
	if err := validateNoCtl("ref", ref); err != nil {
		return err
	}
	// `git check-ref-format` is the authoritative validator.
	cmd := exec.CommandContext(ctx, "git", "check-ref-format", ref)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("invalid ref %q: %s", ref, strings.TrimSpace(string(out)))
	}
	return nil
}

func safeRepoPath(root, p string) (string, error) {
	if err := validateNoCtl("path", p); err != nil {
		return "", err
	}
	clean := filepath.Clean(filepath.FromSlash(p))
	if clean == "." {
		return "", fmt.Errorf("%w: empty path", ErrInvalidPath)
	}
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("%w: absolute path %q", ErrInvalidPath, p)
	}
	abs := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: path traversal %q", ErrInvalidPath, p)
	}
	return abs, nil
}

// PushOptions controls how GitRepo updates the remote ref.
type PushOptions struct {
	// ForceWithLease performs a compare-and-swap update of the remote ref. If the remote ref has moved since
	// ExpectedRemoteHash was observed, the push fails.
	ForceWithLease bool

	// ExpectedRemoteHash is the expected current value of the remote ref (40-hex SHA1 or any rev Git accepts).
	// If empty, Git will use its last known value for the ref (less strict).
	ExpectedRemoteHash string
}

// OpenOptions configures how a GitRepo is opened or cloned.
type OpenOptions struct {
	// URL is the git repository URL
	URL string

	// Ref is the git reference to use (default: refs/dolt/data)
	Ref string

	// LocalPath is an optional local directory for the working copy.
	// If empty, a temporary directory is created.
	LocalPath string
}

// Open opens or clones a git repository for dolt remote operations.
// If the repository doesn't exist locally, it will be cloned.
// If the custom ref doesn't exist, it will be created on first push.
func Open(ctx context.Context, opts OpenOptions) (*GitRepo, error) {
	if opts.URL == "" {
		return nil, errors.New("git URL is required")
	}

	ref := opts.Ref
	if ref == "" {
		ref = DefaultRef
	}
	if err := validateRefName(ctx, ref); err != nil {
		return nil, err
	}
	if err := validateNoCtl("url", opts.URL); err != nil {
		return nil, err
	}

	localPath := opts.LocalPath
	owns := false
	if localPath == "" {
		var err error
		localPath, err = os.MkdirTemp("", "dolt-git-repo-*")
		if err != nil {
			return nil, err
		}
		owns = true
	}

	gr := &GitRepo{
		url:         opts.URL,
		ref:         ref,
		localPath:   localPath,
		ownsWorkdir: owns,
	}

	var err error
	err = gr.openOrCloneToPath(ctx, localPath)
	if err != nil {
		_ = gr.Close()
		return nil, err
	}

	return gr, nil
}

// openOrCloneToPath opens an existing repo or clones to a local path.
func (gr *GitRepo) openOrCloneToPath(ctx context.Context, path string) error {
	// Read config under lock for consistency with other methods.
	gr.mu.RLock()
	url := gr.url
	gr.mu.RUnlock()

	// If this is already a git repo, just fetch the ref.
	if isGitRepo(path) {
		return gr.fetchRef(ctx)
	}

	// Ensure parent dir exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Clone without checkout. This works for empty remotes as well.
	if _, err := gr.runGit(ctx, "", "clone", "--no-checkout", url, path); err != nil {
		return NewCloneError(url, err)
	}

	return gr.fetchRef(ctx)
}

// fetchRef fetches the custom ref from the remote.
func (gr *GitRepo) fetchRef(ctx context.Context) error {
	// Read config under lock for consistency with other methods.
	gr.mu.RLock()
	localPath := gr.localPath
	url := gr.url
	ref := gr.ref
	gr.mu.RUnlock()

	refSpec := fmt.Sprintf("+%s:%s", ref, ref)
	_, err := gr.runGit(ctx, localPath, "fetch", DefaultRemoteName, refSpec)
	if err == nil {
		return nil
	}
	// Missing ref is OK (new remote)
	if isRefNotFoundError(err) {
		return nil
	}
	// "already up to date" isn't an error for git CLI fetch, but keep just in case.
	if strings.Contains(strings.ToLower(err.Error()), "already up to date") {
		return nil
	}
	return NewFetchError(url, ref, err)
}

// isRefNotFoundError checks if an error indicates the ref doesn't exist.
func isRefNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "couldn't find remote ref") ||
		strings.Contains(errStr, "reference not found")
}

// CheckoutRef checks out the custom ref to the worktree.
func (gr *GitRepo) CheckoutRef(ctx context.Context) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// If the ref doesn't exist locally, that's OK (new remote).
	if !gr.hasRef(ctx, gr.ref) {
		return nil
	}

	// Avoid clobbering local changes with a forced checkout.
	statusOut, stErr := gr.runGit(ctx, gr.localPath, "status", "--porcelain")
	if stErr != nil {
		return fmt.Errorf("failed to get status before checkout: %w", stErr)
	}
	if strings.TrimSpace(statusOut) != "" {
		return ErrDirtyWorktree
	}

	hash, err := gr.revParse(ctx, gr.ref)
	if err != nil {
		return fmt.Errorf("failed to resolve ref: %w", err)
	}
	_, err = gr.runGit(ctx, gr.localPath, "checkout", "--detach", "--force", hash)
	if err != nil {
		return fmt.Errorf("failed to checkout: %w", err)
	}
	return nil
}

// ReadFile reads a file from the worktree.
func (gr *GitRepo) ReadFile(path string) ([]byte, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	abs, err := safeRepoPath(gr.localPath, path)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(abs)
}

// WriteFile writes a file to the worktree.
func (gr *GitRepo) WriteFile(path string, data []byte) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// Ensure parent directory exists
	abs, err := safeRepoPath(gr.localPath, path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	if err := os.WriteFile(abs, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", path, err)
	}
	return nil
}

// DeleteFile removes a file from the worktree.
func (gr *GitRepo) DeleteFile(path string) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	abs, err := safeRepoPath(gr.localPath, path)
	if err != nil {
		return err
	}
	return os.Remove(abs)
}

// FileExists checks if a file exists in the worktree.
func (gr *GitRepo) FileExists(path string) (bool, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	abs, err := safeRepoPath(gr.localPath, path)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(abs)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// ListFiles lists files in a directory.
func (gr *GitRepo) ListFiles(dir string) ([]string, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	abs, err := safeRepoPath(gr.localPath, dir)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	return files, nil
}

// Commit creates a new commit with all staged changes.
// Returns the commit hash.
func (gr *GitRepo) Commit(ctx context.Context, message string) (string, error) {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	if err := validateNoCtl("commit message", message); err != nil {
		return "", err
	}

	// Ensure HEAD/worktree is positioned correctly.
	//
	// IMPORTANT: never discard user modifications. We only perform a forced checkout
	// if the worktree is clean AND HEAD is not already at the ref tip.
	if gr.hasRef(ctx, gr.ref) {
		refHash, err := gr.revParse(ctx, gr.ref)
		if err != nil {
			return "", fmt.Errorf("failed to resolve ref: %w", err)
		}

		headHash, err := gr.revParse(ctx, "HEAD")
		if err == nil && headHash != "" && headHash != refHash {
			statusOut, stErr := gr.runGit(ctx, gr.localPath, "status", "--porcelain")
			if stErr != nil {
				return "", fmt.Errorf("failed to get status before checkout: %w", stErr)
			}
			if strings.TrimSpace(statusOut) != "" {
				return "", ErrDirtyWorktree
			}
			if _, err := gr.runGit(ctx, gr.localPath, "checkout", "--detach", "--force", refHash); err != nil {
				return "", fmt.Errorf("failed to checkout ref: %w", err)
			}
		}
	} else {
		// Create an orphan branch for the first commit on this custom ref.
		// We never want to base Dolt remote data on the user's normal branches.
		tmpBranch := fmt.Sprintf("dolt-temp-%d-%d", os.Getpid(), time.Now().UnixNano())
		if _, err := gr.runGit(ctx, gr.localPath, "checkout", "--orphan", tmpBranch); err != nil {
			return "", fmt.Errorf("failed to create orphan branch: %w", err)
		}
		defer func() {
			_, _ = gr.runGit(ctx, gr.localPath, "checkout", "--detach", "--force", "HEAD")
			_, _ = gr.runGit(ctx, gr.localPath, "branch", "-D", tmpBranch)
		}()
	}

	// Stage all changes
	if _, err := gr.runGit(ctx, gr.localPath, "add", "-A"); err != nil {
		return "", fmt.Errorf("failed to stage changes: %w", err)
	}

	// Check if there are changes to commit
	porcelain, err := gr.runGit(ctx, gr.localPath, "status", "--porcelain")
	if err != nil {
		return "", fmt.Errorf("failed to get status: %w", err)
	}
	if strings.TrimSpace(porcelain) == "" {
		return "", ErrNothingToCommit
	}

	// Create commit
	commitArgs := []string{
		"-c", "user.name=Dolt",
		"-c", "user.email=dolt@dolthub.com",
		"-c", "commit.gpgsign=false",
		"commit",
		"-m", message,
		"--no-gpg-sign",
		"--date", time.Now().Format(time.RFC3339),
	}
	if _, err := gr.runGit(ctx, gr.localPath, commitArgs...); err != nil {
		return "", fmt.Errorf("failed to commit: %w", err)
	}

	hash, err := gr.revParse(ctx, "HEAD")
	if err != nil {
		return "", fmt.Errorf("failed to resolve HEAD: %w", err)
	}

	// Update the custom ref to point to the new commit
	if _, err := gr.runGit(ctx, gr.localPath, "update-ref", gr.ref, "HEAD"); err != nil {
		return "", fmt.Errorf("failed to update ref: %w", err)
	}

	return hash, nil
}

// Push pushes the custom ref to the remote.
func (gr *GitRepo) Push(ctx context.Context) error {
	return gr.PushWithOptions(ctx, PushOptions{})
}

// PushWithLease pushes the custom ref using `--force-with-lease`. If expectedRemoteHash is non-empty, it is
// used as the expected current value of the remote ref (stronger CAS semantics).
func (gr *GitRepo) PushWithLease(ctx context.Context, expectedRemoteHash string) error {
	return gr.PushWithOptions(ctx, PushOptions{
		ForceWithLease:     true,
		ExpectedRemoteHash: expectedRemoteHash,
	})
}

// PushWithOptions pushes the custom ref to the remote.
func (gr *GitRepo) PushWithOptions(ctx context.Context, opts PushOptions) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	if err := validateRefName(ctx, gr.ref); err != nil {
		return err
	}
	if err := validateNoCtl("expected remote hash", opts.ExpectedRemoteHash); err != nil {
		return err
	}

	refSpec := fmt.Sprintf("%s:%s", gr.ref, gr.ref)

	args := []string{"push"}
	if opts.ForceWithLease {
		if opts.ExpectedRemoteHash != "" {
			args = append(args, fmt.Sprintf("--force-with-lease=%s:%s", gr.ref, opts.ExpectedRemoteHash))
		} else {
			args = append(args, fmt.Sprintf("--force-with-lease=%s", gr.ref))
		}
	}
	args = append(args, DefaultRemoteName, refSpec)

	_, err := gr.runGit(ctx, gr.localPath, args...)
	if err != nil {
		return NewPushError(gr.url, gr.ref, err)
	}
	return nil
}

// CurrentCommit returns the hash of the current commit on the custom ref.
func (gr *GitRepo) CurrentCommit(ctx context.Context) (string, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	if !gr.hasRef(ctx, gr.ref) {
		return "", nil
	}
	return gr.revParse(ctx, gr.ref)
}

// Fetch fetches the latest changes from the remote.
func (gr *GitRepo) Fetch(ctx context.Context) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	return gr.fetchRef(ctx)
}

// URL returns the repository URL.
func (gr *GitRepo) URL() string {
	return gr.url
}

// Ref returns the git reference being used.
func (gr *GitRepo) Ref() string {
	return gr.ref
}

// Close cleans up resources. For in-memory repos, this is a no-op.
// For on-disk repos, this releases file handles.
func (gr *GitRepo) Close() error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	if gr.localPath == "" {
		return nil
	}
	if gr.ownsWorkdir && gr.localPath != "" {
		_ = os.RemoveAll(gr.localPath)
	}
	gr.localPath = ""
	gr.ownsWorkdir = false
	return nil
}

// InitRemote initializes a git repository as a dolt remote.
// This creates the .dolt_remote directory structure and README.
// It is idempotent - safe to call multiple times.
func (gr *GitRepo) InitRemote(ctx context.Context) error {
	// Check if already initialized
	exists, err := gr.FileExists(DoltRemoteDir + "/README.md")
	if err != nil {
		return err
	}
	if exists {
		// Already initialized
		return nil
	}

	// Create directory structure
	if err := gr.WriteFile(DoltRemoteDir+"/README.md", []byte(doltRemoteReadme)); err != nil {
		return fmt.Errorf("failed to create README: %w", err)
	}

	// Create empty data directory marker
	if err := gr.WriteFile(DoltRemoteDataDir+"/.gitkeep", []byte("")); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Commit the initialization
	_, err = gr.Commit(ctx, "Initialize dolt remote")
	if err != nil && !errors.Is(err, ErrNothingToCommit) {
		return fmt.Errorf("failed to commit initialization: %w", err)
	}

	// Push to remote
	if err := gr.Push(ctx); err != nil {
		return fmt.Errorf("failed to push initialization: %w", err)
	}

	return nil
}

// IsInitialized checks if the repository has been initialized as a dolt remote.
func (gr *GitRepo) IsInitialized() (bool, error) {
	return gr.FileExists(DoltRemoteDir + "/README.md")
}

const doltRemoteReadme = `# Dolt Remote Data

This directory contains Dolt database remote storage data.

**WARNING**: Do not manually modify files in this directory.

This data is managed by Dolt and uses a custom git ref (` + "`refs/dolt/data`" + `)
that is not part of the normal branch structure.

For more information, visit: https://docs.dolthub.com/
`

func isGitRepo(path string) bool {
	gitDir := filepath.Join(path, ".git")
	if st, err := os.Stat(gitDir); err == nil && st.IsDir() {
		return true
	}
	// For worktrees / alternative gitdir, rely on git itself.
	cmd := exec.Command("git", "-C", path, "rev-parse", "--git-dir")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	return cmd.Run() == nil
}

func (gr *GitRepo) runGit(ctx context.Context, dir string, args ...string) (string, error) {
	// Allow callers to pass dir="" for commands that don't support -C.
	fullArgs := args
	if dir != "" {
		fullArgs = append([]string{"-C", dir}, args...)
	}
	cmd := exec.CommandContext(ctx, "git", fullArgs...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Include output in error for easier classification/debugging.
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func (gr *GitRepo) hasRef(ctx context.Context, ref string) bool {
	// show-ref --verify exits non-zero if missing
	_, err := gr.runGit(ctx, gr.localPath, "show-ref", "--verify", "--quiet", ref)
	return err == nil
}

func (gr *GitRepo) revParse(ctx context.Context, rev string) (string, error) {
	out, err := gr.runGit(ctx, gr.localPath, "rev-parse", rev)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}
