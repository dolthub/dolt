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

package blobstore

import (
	"context"
	"fmt"
	"io"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

// GitBlobstore is a Blobstore implementation backed by a git repository's object
// database (bare repo or .git directory). It stores keys as paths within the tree
// of the commit referenced by a git ref (e.g. refs/dolt/data).
//
// This initial implementation is intentionally READ-ONLY. Write-path methods
// (Put / CheckAndPut / Concatenate) return an explicit unimplemented error while
// we lock down read behavior for manifests and table files.
type GitBlobstore struct {
	gitDir string
	ref    string
	runner *git.Runner
}

var _ Blobstore = (*GitBlobstore)(nil)

// NewGitBlobstore creates a new read-only GitBlobstore rooted at |gitDir| and |ref|.
// |gitDir| should point at a bare repo directory or a .git directory.
func NewGitBlobstore(gitDir, ref string) (*GitBlobstore, error) {
	r, err := git.NewRunner(gitDir)
	if err != nil {
		return nil, err
	}
	return &GitBlobstore{gitDir: gitDir, ref: ref, runner: r}, nil
}

func (gbs *GitBlobstore) Path() string {
	return fmt.Sprintf("%s@%s", gbs.gitDir, gbs.ref)
}

func (gbs *GitBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	commit, ok, err := git.TryResolveRefCommit(ctx, gbs.runner, gbs.ref)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	_, err = git.ResolvePathBlob(ctx, gbs.runner, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (gbs *GitBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	commit, ok, err := git.TryResolveRefCommit(ctx, gbs.runner, gbs.ref)
	if err != nil {
		return nil, 0, "", err
	}
	if !ok {
		return nil, 0, "", NotFound{Key: key}
	}

	blobOID, err := git.ResolvePathBlob(ctx, gbs.runner, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return nil, 0, commit.String(), NotFound{Key: key}
		}
		return nil, 0, commit.String(), err
	}

	sz, err := git.BlobSize(ctx, gbs.runner, blobOID)
	if err != nil {
		return nil, 0, commit.String(), err
	}

	rc, err := git.BlobReader(ctx, gbs.runner, blobOID)
	if err != nil {
		return nil, 0, commit.String(), err
	}

	// Implement BlobRange by slicing the streamed blob contents.
	if br.isAllRange() {
		return rc, uint64(sz), commit.String(), nil
	}

	pos := br.positiveRange(sz)
	if pos.offset < 0 || pos.offset > sz {
		_ = rc.Close()
		return nil, uint64(sz), commit.String(), fmt.Errorf("invalid BlobRange offset %d for blob of size %d", pos.offset, sz)
	}
	if pos.length < 0 {
		_ = rc.Close()
		return nil, uint64(sz), commit.String(), fmt.Errorf("invalid BlobRange length %d", pos.length)
	}
	if pos.length == 0 {
		// Read from offset to end.
		pos.length = sz - pos.offset
	}
	// Clamp to end (defensive; positiveRange should already do this).
	if pos.offset+pos.length > sz {
		pos.length = sz - pos.offset
	}

	// Skip to offset.
	if pos.offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, pos.offset); err != nil {
			_ = rc.Close()
			return nil, uint64(sz), commit.String(), err
		}
	}

	return &limitReadCloser{r: io.LimitReader(rc, pos.length), c: rc}, uint64(sz), commit.String(), nil
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitReadCloser) Close() error               { return l.c.Close() }

func (gbs *GitBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return "", fmt.Errorf("%w: GitBlobstore.Put", git.ErrUnimplemented)
}

func (gbs *GitBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	return "", fmt.Errorf("%w: GitBlobstore.CheckAndPut", git.ErrUnimplemented)
}

func (gbs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return "", fmt.Errorf("%w: GitBlobstore.Concatenate", git.ErrUnimplemented)
}

