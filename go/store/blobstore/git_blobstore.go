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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

// GitBlobstore is a Blobstore implementation backed by a git repository's object
// database (bare repo or .git directory). It stores keys as paths within the tree
// of the commit referenced by a git ref (e.g. refs/dolt/data).
//
// This implementation is being developed in phases. Read paths are implemented first,
// then write paths are added incrementally. At the moment, Put is implemented, while
// CheckAndPut and Concatenate are still unimplemented.
type GitBlobstore struct {
	gitDir string
	ref    string
	runner *git.Runner
	api    git.GitAPI
	// identity, when non-nil, is used as the author/committer identity for new commits.
	// When nil, we prefer whatever identity git derives from env/config, falling back
	// to a deterministic default only if git reports the identity is missing.
	identity *git.Identity
}

var _ Blobstore = (*GitBlobstore)(nil)

// NewGitBlobstore creates a new GitBlobstore rooted at |gitDir| and |ref|.
// |gitDir| should point at a bare repo directory or a .git directory. Put is implemented,
// while CheckAndPut and Concatenate are still unimplemented (see type-level docs).
func NewGitBlobstore(gitDir, ref string) (*GitBlobstore, error) {
	return NewGitBlobstoreWithIdentity(gitDir, ref, nil)
}

// NewGitBlobstoreWithIdentity creates a GitBlobstore rooted at |gitDir| and |ref|, optionally
// forcing an author/committer identity for write paths.
func NewGitBlobstoreWithIdentity(gitDir, ref string, identity *git.Identity) (*GitBlobstore, error) {
	r, err := git.NewRunner(gitDir)
	if err != nil {
		return nil, err
	}
	return &GitBlobstore{gitDir: gitDir, ref: ref, runner: r, api: git.NewGitAPIImpl(r), identity: identity}, nil
}

func (gbs *GitBlobstore) Path() string {
	return fmt.Sprintf("%s@%s", gbs.gitDir, gbs.ref)
}

func (gbs *GitBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return false, err
	}
	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	_, err = gbs.api.ResolvePathBlob(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (gbs *GitBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return nil, 0, "", err
	}
	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return nil, 0, "", err
	}
	if !ok {
		// If the ref doesn't exist, treat the manifest as missing (empty store),
		// but surface a hard error for other keys: the store itself is missing.
		if key == "manifest" {
			return nil, 0, "", NotFound{Key: key}
		}
		return nil, 0, "", &git.RefNotFoundError{Ref: gbs.ref}
	}

	blobOID, err := gbs.api.ResolvePathBlob(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return nil, 0, commit.String(), NotFound{Key: key}
		}
		return nil, 0, commit.String(), err
	}

	sz, err := gbs.api.BlobSize(ctx, blobOID)
	if err != nil {
		return nil, 0, commit.String(), err
	}

	// TODO(gitblobstore): This streaming implementation is correct but may be slow for workloads
	// that do many small ranged reads (e.g. table index/footer reads). Consider caching/materializing
	// blobs to a local file (or using a batched git cat-file mode) to serve ranges efficiently.
	rc, err := gbs.api.BlobReader(ctx, blobOID)
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
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	// Hash the contents once. If we need to retry due to concurrent updates to |gbs.ref|,
	// we can reuse the blob OID without re-reading |reader|.
	blobOID, err := gbs.api.HashObject(ctx, reader)
	if err != nil {
		return "", err
	}

	// Make Put resilient to concurrent writers updating unrelated keys by using a CAS loop
	// under the hood. This matches typical object-store semantics more closely than an
	// unconditional ref update (which could clobber other keys).
	const maxRetries = 31 // 32 total attempts (initial + retries)
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 5 * time.Millisecond
	bo.Multiplier = 2
	bo.MaxInterval = 320 * time.Millisecond
	bo.RandomizationFactor = 0 // deterministic; can add jitter later if needed
	bo.Reset()
	policy := backoff.WithContext(backoff.WithMaxRetries(bo, maxRetries), ctx)

	var ver string
	op := func() error {
		parent, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}

		newCommit, msg, err := gbs.buildPutCommit(ctx, parent, ok, key, blobOID)
		if err != nil {
			return backoff.Permanent(err)
		}

		if !ok {
			// Best-effort ref creation. If a concurrent writer created the ref first, retry
			// and take the normal CAS path on the new head.
			if err := gbs.api.UpdateRef(ctx, gbs.ref, newCommit, msg); err != nil {
				if gbs.refAdvanced(ctx, parent) {
					return err
				}
				return backoff.Permanent(err)
			}
			ver = newCommit.String()
			return nil
		}

		err = gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, parent, msg)
		if err == nil {
			ver = newCommit.String()
			return nil
		}

		// If the ref changed since we read |parent|, retry on the new head. Otherwise
		// surface the error (e.g. permissions, corruption).
		if gbs.refAdvanced(ctx, parent) {
			return err
		}
		return backoff.Permanent(err)
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}
	return ver, nil
}

func (gbs *GitBlobstore) buildPutCommit(ctx context.Context, parent git.OID, hasParent bool, key string, blobOID git.OID) (git.OID, string, error) {
	_, indexFile, cleanup, err := newTempIndex()
	if err != nil {
		return "", "", err
	}
	defer cleanup()

	if hasParent {
		if err := gbs.api.ReadTree(ctx, parent, indexFile); err != nil {
			return "", "", err
		}
	} else {
		if err := gbs.api.ReadTreeEmpty(ctx, indexFile); err != nil {
			return "", "", err
		}
	}

	if err := gbs.api.UpdateIndexCacheInfo(ctx, indexFile, "100644", blobOID, key); err != nil {
		return "", "", err
	}

	treeOID, err := gbs.api.WriteTree(ctx, indexFile)
	if err != nil {
		return "", "", err
	}

	var parentPtr *git.OID
	if hasParent && parent != "" {
		p := parent
		parentPtr = &p
	}
	msg := fmt.Sprintf("gitblobstore: put %s", key)

	// Prefer git's default identity from env/config when not explicitly configured.
	commitOID, err := gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, gbs.identity)
	if err != nil && gbs.identity == nil && isMissingGitIdentityErr(err) {
		commitOID, err = gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, defaultGitBlobstoreIdentity())
	}
	if err != nil {
		return "", "", err
	}

	return commitOID, msg, nil
}

func defaultGitBlobstoreIdentity() *git.Identity {
	// Deterministic fallback identity for environments without git identity configured.
	return &git.Identity{Name: "dolt gitblobstore", Email: "gitblobstore@dolt.invalid"}
}

func isMissingGitIdentityErr(err error) bool {
	var ce *git.CmdError
	if !errors.As(err, &ce) {
		return false
	}
	msg := strings.ToLower(string(ce.Output))
	// Common git messages:
	// - "Author identity unknown"
	// - "fatal: unable to auto-detect email address"
	// - "fatal: empty ident name"
	return strings.Contains(msg, "author identity unknown") ||
		strings.Contains(msg, "unable to auto-detect email address") ||
		strings.Contains(msg, "empty ident name")
}

func newTempIndex() (dir, indexFile string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "dolt-gitblobstore-index-")
	if err != nil {
		return "", "", nil, err
	}
	indexFile = filepath.Join(dir, "index")
	cleanup = func() { _ = os.RemoveAll(dir) }
	return dir, indexFile, cleanup, nil
}

func (gbs *GitBlobstore) refAdvanced(ctx context.Context, old git.OID) bool {
	if ctx.Err() != nil {
		return false
	}
	cur, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	return err == nil && ok && cur != old
}

func (gbs *GitBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	if _, err := normalizeGitTreePath(key); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%w: GitBlobstore.CheckAndPut", git.ErrUnimplemented)
}

func (gbs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	if _, err := normalizeGitTreePath(key); err != nil {
		return "", err
	}
	for _, src := range sources {
		if _, err := normalizeGitTreePath(src); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("%w: GitBlobstore.Concatenate", git.ErrUnimplemented)
}

// normalizeGitTreePath normalizes and validates a blobstore key for use as a git tree path.
//
// Rules:
// - convert Windows-style separators: "\" -> "/"
// - disallow absolute paths (leading "/")
// - disallow empty segments and trailing "/"
// - disallow "." and ".." segments
// - disallow NUL bytes
func normalizeGitTreePath(key string) (string, error) {
	if strings.ContainsRune(key, '\x00') {
		return "", fmt.Errorf("invalid git blobstore key (NUL byte): %q", key)
	}
	key = strings.ReplaceAll(key, "\\", "/")
	if key == "" {
		return "", fmt.Errorf("invalid git blobstore key (empty)")
	}
	if strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("invalid git blobstore key (absolute path): %q", key)
	}

	parts := strings.Split(key, "/")
	for _, p := range parts {
		if p == "" {
			return "", fmt.Errorf("invalid git blobstore key (empty path segment): %q", key)
		}
		if p == "." || p == ".." {
			return "", fmt.Errorf("invalid git blobstore key (path traversal): %q", key)
		}
		if strings.ContainsRune(p, '\x00') {
			return "", fmt.Errorf("invalid git blobstore key (NUL byte): %q", key)
		}
	}
	return key, nil
}
