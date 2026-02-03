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
	// If this is already a git repo, just fetch the ref.
	if isGitRepo(path) {
		return gr.fetchRef(ctx)
	}

	// Ensure parent dir exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Clone without checkout. This works for empty remotes as well.
	if _, err := gr.runGit(ctx, "", "clone", "--no-checkout", gr.url, path); err != nil {
		return NewCloneError(gr.url, err)
	}

	return gr.fetchRef(ctx)
}

// fetchRef fetches the custom ref from the remote.
func (gr *GitRepo) fetchRef(ctx context.Context) error {
	refSpec := fmt.Sprintf("+%s:%s", gr.ref, gr.ref)
	_, err := gr.runGit(ctx, gr.localPath, "fetch", DefaultRemoteName, refSpec)
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
	return NewFetchError(gr.url, gr.ref, err)
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

	return os.ReadFile(filepath.Join(gr.localPath, filepath.FromSlash(path)))
}

// WriteFile writes a file to the worktree.
func (gr *GitRepo) WriteFile(path string, data []byte) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// Ensure parent directory exists
	abs := filepath.Join(gr.localPath, filepath.FromSlash(path))
	dir := filepath.Dir(abs)
	if dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
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

	return os.Remove(filepath.Join(gr.localPath, filepath.FromSlash(path)))
}

// FileExists checks if a file exists in the worktree.
func (gr *GitRepo) FileExists(path string) (bool, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	_, err := os.Stat(filepath.Join(gr.localPath, filepath.FromSlash(path)))
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

	entries, err := os.ReadDir(filepath.Join(gr.localPath, filepath.FromSlash(dir)))
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

	// Ensure HEAD/worktree is positioned correctly.
	// If the custom ref exists, base on it. Otherwise, start a new orphan branch.
	if gr.hasRef(ctx, gr.ref) {
		hash, err := gr.revParse(ctx, gr.ref)
		if err != nil {
			return "", fmt.Errorf("failed to resolve ref: %w", err)
		}
		if _, err := gr.runGit(ctx, gr.localPath, "checkout", "--detach", "--force", hash); err != nil {
			return "", fmt.Errorf("failed to checkout ref: %w", err)
		}
	} else {
		// Create an orphan branch for the first commit on this custom ref.
		// We never want to base Dolt remote data on the user's normal branches.
		if _, err := gr.runGit(ctx, gr.localPath, "checkout", "--orphan", "dolt-temp"); err != nil {
			return "", fmt.Errorf("failed to create orphan branch: %w", err)
		}
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

	// If we created the orphan branch, detach and delete it (the commit is reachable via the custom ref).
	_, _ = gr.runGit(ctx, gr.localPath, "checkout", "--detach", "--force", "HEAD")
	_, _ = gr.runGit(ctx, gr.localPath, "branch", "-D", "dolt-temp")

	return hash, nil
}

// Push pushes the custom ref to the remote.
func (gr *GitRepo) Push(ctx context.Context) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	refSpec := fmt.Sprintf("%s:%s", gr.ref, gr.ref)
	_, err := gr.runGit(ctx, gr.localPath, "push", DefaultRemoteName, refSpec)
	if err != nil {
		return NewPushError(gr.url, gr.ref, err)
	}
	return nil
}

// CurrentCommit returns the hash of the current commit on the custom ref.
func (gr *GitRepo) CurrentCommit() (string, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	ctx := context.Background()
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

	if gr.ownsWorkdir && gr.localPath != "" {
		_ = os.RemoveAll(gr.localPath)
	}
	gr.localPath = ""
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
