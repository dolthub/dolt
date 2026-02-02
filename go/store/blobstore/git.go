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

package blobstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/gitremote"
)

// GitBlobstore is a Blobstore implementation that uses a git repository as the backing store.
// It wraps a LocalBlobstore for the actual file operations, storing blobs under
// .dolt_remote/data/ in the git worktree. Sync operations (commit + push) are handled
// separately from individual blob operations.
type GitBlobstore struct {
	local     *LocalBlobstore
	repo      *gitremote.GitRepo
	ref       string
	repoURL   string
	localPath string
	mu        sync.RWMutex
}

var _ Blobstore = &GitBlobstore{}

// NewGitBlobstore creates a new GitBlobstore for the given repository URL.
// The localPath is used as a cache directory for git operations.
// If localPath is empty, a temporary directory will be created.
func NewGitBlobstore(ctx context.Context, repoURL, ref, localPath string) (*GitBlobstore, error) {
	if localPath == "" {
		var err error
		localPath, err = os.MkdirTemp("", "dolt-git-blobstore-*")
		if err != nil {
			return nil, err
		}
	}

	if ref == "" {
		ref = gitremote.DefaultRef
	}

	// Detect authentication
	auth, err := gitremote.DetectAuth(repoURL)
	if err != nil {
		return nil, err
	}

	// Open or clone the repository
	repo, err := gitremote.Open(ctx, gitremote.OpenOptions{
		URL:       repoURL,
		Ref:       ref,
		Auth:      auth,
		LocalPath: localPath,
	})
	if err != nil {
		return nil, err
	}

	// Checkout the ref to populate the worktree
	if err := repo.CheckoutRef(ctx); err != nil {
		return nil, err
	}

	// Create the data directory for blobs
	dataDir := filepath.Join(localPath, gitremote.DoltRemoteDataDir)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Create a LocalBlobstore pointing to the data directory
	local := NewLocalBlobstore(dataDir)

	return &GitBlobstore{
		local:     local,
		repo:      repo,
		ref:       ref,
		repoURL:   repoURL,
		localPath: localPath,
	}, nil
}

// Path returns the git repository URL.
func (bs *GitBlobstore) Path() string {
	return bs.repoURL
}

// Exists returns true if a blob with the given key exists.
func (bs *GitBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.local.Exists(ctx, key)
}

// Get retrieves a blob by key, returning a reader for the specified byte range.
func (bs *GitBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.local.Get(ctx, key, br)
}

// Put stores a blob with the given key in the local worktree.
// This does NOT commit or push - call Sync() to persist changes to the remote.
func (bs *GitBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.local.Put(ctx, key, totalSize, reader)
}

// CheckAndPut stores a blob only if the current version matches expectedVersion.
// For git blobstores, this also commits and pushes all pending changes to ensure
// the remote is in sync with the local state.
func (bs *GitBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// First, do the local CheckAndPut
	ver, err := bs.local.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
	if err != nil {
		return "", err
	}

	// Now commit and push all pending changes (including this one)
	_, commitErr := bs.repo.Commit(ctx, "Update "+key)
	if commitErr != nil && commitErr != gitremote.ErrNothingToCommit {
		return "", commitErr
	}

	if commitErr != gitremote.ErrNothingToCommit {
		if pushErr := bs.repo.Push(ctx); pushErr != nil {
			return "", pushErr
		}
	}

	return ver, nil
}

// Concatenate creates a new blob by concatenating the contents of the source blobs.
func (bs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.local.Concatenate(ctx, key, sources)
}

// Sync commits all pending changes and pushes to the remote.
// This should be called after a batch of Put operations to persist changes.
func (bs *GitBlobstore) Sync(ctx context.Context, message string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Commit changes
	_, err := bs.repo.Commit(ctx, message)
	if err != nil {
		if err == gitremote.ErrNothingToCommit {
			return nil // Nothing to sync
		}
		return err
	}

	// Push to remote
	return bs.repo.Push(ctx)
}

// Fetch pulls the latest changes from the remote.
func (bs *GitBlobstore) Fetch(ctx context.Context) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if err := bs.repo.Fetch(ctx); err != nil {
		return err
	}
	return bs.repo.CheckoutRef(ctx)
}

// Close releases resources associated with this blobstore.
func (bs *GitBlobstore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.repo != nil {
		return bs.repo.Close()
	}
	return nil
}

// Ref returns the git ref being used.
func (bs *GitBlobstore) Ref() string {
	return bs.ref
}

// LocalPath returns the local cache directory path.
func (bs *GitBlobstore) LocalPath() string {
	return bs.localPath
}
