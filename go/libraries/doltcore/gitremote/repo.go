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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/memory"
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
// It wraps go-git and handles custom ref operations for storing dolt data.
type GitRepo struct {
	url       string
	ref       plumbing.ReferenceName
	auth      transport.AuthMethod
	repo      *git.Repository
	worktree  *git.Worktree
	fs        billy.Filesystem
	localPath string // empty if using in-memory storage
	mu        sync.RWMutex
}

// OpenOptions configures how a GitRepo is opened or cloned.
type OpenOptions struct {
	// URL is the git repository URL
	URL string

	// Ref is the git reference to use (default: refs/dolt/data)
	Ref string

	// Auth is the authentication method (nil for anonymous)
	Auth transport.AuthMethod

	// LocalPath is an optional local directory for the working copy.
	// If empty, an in-memory filesystem is used.
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
	refName := plumbing.ReferenceName(ref)

	gr := &GitRepo{
		url:       opts.URL,
		ref:       refName,
		auth:      opts.Auth,
		localPath: opts.LocalPath,
	}

	var err error
	if opts.LocalPath != "" {
		err = gr.openOrCloneToPath(ctx, opts.LocalPath)
	} else {
		err = gr.openOrCloneInMemory(ctx)
	}

	if err != nil {
		return nil, err
	}

	return gr, nil
}

// openOrCloneToPath opens an existing repo or clones to a local path.
func (gr *GitRepo) openOrCloneToPath(ctx context.Context, path string) error {
	// Try to open existing repository
	repo, err := git.PlainOpen(path)
	if err == nil {
		gr.repo = repo
		gr.fs = osfs.New(path)
		wt, err := repo.Worktree()
		if err != nil {
			return fmt.Errorf("failed to get worktree: %w", err)
		}
		gr.worktree = wt
		return gr.fetchRef(ctx)
	}

	// Need to clone or init
	if !errors.Is(err, git.ErrRepositoryNotExists) {
		return fmt.Errorf("failed to open repository: %w", err)
	}

	// Create directory if needed
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Try to clone the repository
	repo, err = git.PlainCloneContext(ctx, path, false, &git.CloneOptions{
		URL:  gr.url,
		Auth: gr.auth,
		// Don't check out any branch - we'll work with our custom ref
		NoCheckout: true,
	})

	// Handle empty remote repository - init locally and add remote
	if err != nil && isEmptyRepoError(err) {
		repo, err = gr.initWithRemote(path)
		if err != nil {
			return fmt.Errorf("failed to init repository: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}

	gr.repo = repo
	gr.fs = osfs.New(path)
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}
	gr.worktree = wt

	return gr.fetchRef(ctx)
}

// initWithRemote initializes a new repository and adds the remote.
func (gr *GitRepo) initWithRemote(path string) (*git.Repository, error) {
	repo, err := git.PlainInit(path, false)
	if err != nil {
		return nil, err
	}

	_, err = repo.CreateRemote(&config.RemoteConfig{
		Name: DefaultRemoteName,
		URLs: []string{gr.url},
	})
	if err != nil {
		return nil, err
	}

	return repo, nil
}

// isEmptyRepoError checks if the error indicates an empty remote repository.
func isEmptyRepoError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errors.Is(err, transport.ErrEmptyRemoteRepository) ||
		contains(errStr, "remote repository is empty") ||
		// Local bare repos with no commits return "reference not found"
		contains(errStr, "reference not found")
}

// openOrCloneInMemory opens or clones to an in-memory filesystem.
func (gr *GitRepo) openOrCloneInMemory(ctx context.Context) error {
	storer := memory.NewStorage()
	fs := memfs.New()

	repo, err := git.CloneContext(ctx, storer, fs, &git.CloneOptions{
		URL:        gr.url,
		Auth:       gr.auth,
		NoCheckout: true,
	})

	// Handle empty remote repository - init in memory and add remote
	if err != nil && isEmptyRepoError(err) {
		storer = memory.NewStorage()
		fs = memfs.New()
		repo, err = git.Init(storer, fs)
		if err != nil {
			return fmt.Errorf("failed to init repository: %w", err)
		}

		_, err = repo.CreateRemote(&config.RemoteConfig{
			Name: DefaultRemoteName,
			URLs: []string{gr.url},
		})
		if err != nil {
			return fmt.Errorf("failed to create remote: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}

	gr.repo = repo
	gr.fs = fs
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}
	gr.worktree = wt

	return gr.fetchRef(ctx)
}

// fetchRef fetches the custom ref from the remote.
func (gr *GitRepo) fetchRef(ctx context.Context) error {
	refSpec := config.RefSpec(fmt.Sprintf("+%s:%s", gr.ref, gr.ref))

	err := gr.repo.FetchContext(ctx, &git.FetchOptions{
		RemoteName: DefaultRemoteName,
		RefSpecs:   []config.RefSpec{refSpec},
		Auth:       gr.auth,
		Force:      true,
	})

	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) && !errors.Is(err, transport.ErrEmptyRemoteRepository) {
		// Check if the ref doesn't exist yet (new remote)
		if isRefNotFoundError(err) {
			// This is OK - the ref will be created on first push
			return nil
		}
		return fmt.Errorf("failed to fetch ref %s: %w", gr.ref, err)
	}

	return nil
}

// isRefNotFoundError checks if an error indicates the ref doesn't exist.
func isRefNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errors.Is(err, plumbing.ErrReferenceNotFound) ||
		// go-git returns various error messages for missing refs
		contains(errStr, "couldn't find remote ref") ||
		contains(errStr, "reference not found")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// CheckoutRef checks out the custom ref to the worktree.
func (gr *GitRepo) CheckoutRef(ctx context.Context) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// Try to resolve the ref
	ref, err := gr.repo.Reference(gr.ref, true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			// Ref doesn't exist yet - create empty worktree
			return nil
		}
		return fmt.Errorf("failed to resolve ref: %w", err)
	}

	err = gr.worktree.Checkout(&git.CheckoutOptions{
		Hash:  ref.Hash(),
		Force: true,
	})
	if err != nil {
		return fmt.Errorf("failed to checkout: %w", err)
	}

	return nil
}

// ReadFile reads a file from the worktree.
func (gr *GitRepo) ReadFile(path string) ([]byte, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	f, err := gr.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}

// WriteFile writes a file to the worktree.
func (gr *GitRepo) WriteFile(path string, data []byte) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if dir != "." && dir != "/" {
		if err := gr.fs.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	f, err := gr.fs.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", path, err)
	}

	return nil
}

// DeleteFile removes a file from the worktree.
func (gr *GitRepo) DeleteFile(path string) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	return gr.fs.Remove(path)
}

// FileExists checks if a file exists in the worktree.
func (gr *GitRepo) FileExists(path string) (bool, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	_, err := gr.fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ListFiles lists files in a directory.
func (gr *GitRepo) ListFiles(dir string) ([]string, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	entries, err := gr.fs.ReadDir(dir)
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
func (gr *GitRepo) Commit(ctx context.Context, message string) (plumbing.Hash, error) {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// Stage all changes
	if err := gr.worktree.AddGlob("."); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to stage changes: %w", err)
	}

	// Check if there are changes to commit
	status, err := gr.worktree.Status()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to get status: %w", err)
	}

	if status.IsClean() {
		return plumbing.ZeroHash, ErrNothingToCommit
	}

	// Create commit
	hash, err := gr.worktree.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Dolt",
			Email: "dolt@dolthub.com",
			When:  time.Now(),
		},
	})
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to commit: %w", err)
	}

	// Update the custom ref to point to the new commit
	ref := plumbing.NewHashReference(gr.ref, hash)
	if err := gr.repo.Storer.SetReference(ref); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to update ref: %w", err)
	}

	return hash, nil
}

// Push pushes the custom ref to the remote.
func (gr *GitRepo) Push(ctx context.Context) error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	refSpec := config.RefSpec(fmt.Sprintf("%s:%s", gr.ref, gr.ref))

	err := gr.repo.PushContext(ctx, &git.PushOptions{
		RemoteName: DefaultRemoteName,
		RefSpecs:   []config.RefSpec{refSpec},
		Auth:       gr.auth,
	})

	if err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			return nil
		}
		return fmt.Errorf("failed to push: %w", err)
	}

	return nil
}

// CurrentCommit returns the hash of the current commit on the custom ref.
func (gr *GitRepo) CurrentCommit() (plumbing.Hash, error) {
	gr.mu.RLock()
	defer gr.mu.RUnlock()

	ref, err := gr.repo.Reference(gr.ref, true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return plumbing.ZeroHash, nil
		}
		return plumbing.ZeroHash, err
	}

	return ref.Hash(), nil
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
	return string(gr.ref)
}

// Close cleans up resources. For in-memory repos, this is a no-op.
// For on-disk repos, this releases file handles.
func (gr *GitRepo) Close() error {
	gr.mu.Lock()
	defer gr.mu.Unlock()

	// go-git doesn't have explicit close, but we clear references
	gr.repo = nil
	gr.worktree = nil
	gr.fs = nil

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
