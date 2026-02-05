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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitbs "github.com/dolthub/dolt/go/store/blobstore/internal/gitbs"
)

// GitBlobstore is a Blobstore implementation backed by a git repository's object
// database (bare repo or .git directory). It stores keys as paths within the tree
// of the commit referenced by a git ref (e.g. refs/dolt/data).
//
// This implementation is being developed in phases. Read paths were implemented first,
// then write paths were added incrementally.
type GitBlobstore struct {
	gitDir string
	ref    string
	runner *git.Runner
	api    git.GitAPI
	// identity, when non-nil, is used as the author/committer identity for new commits.
	// When nil, we prefer whatever identity git derives from env/config, falling back
	// to a deterministic default only if git reports the identity is missing.
	identity *git.Identity
	// maxPartSize, when non-zero, enables the chunked-object representation for objects
	// written by Put/CheckAndPut/Concatenate. When enabled, no single part blob created
	// by this blobstore should exceed maxPartSize bytes.
	//
	// A zero value means "disabled" (store values inline as a single git blob).
	maxPartSize uint64
}

var _ Blobstore = (*GitBlobstore)(nil)

// NewGitBlobstore creates a new GitBlobstore rooted at |gitDir| and |ref|.
// |gitDir| should point at a bare repo directory or a .git directory. Put is implemented,
// while CheckAndPut and Concatenate are still unimplemented (see type-level docs).
func NewGitBlobstore(gitDir, ref string) (*GitBlobstore, error) {
	return NewGitBlobstoreWithOptions(gitDir, ref, GitBlobstoreOptions{})
}

// NewGitBlobstoreWithIdentity creates a GitBlobstore rooted at |gitDir| and |ref|, optionally
// forcing an author/committer identity for write paths.
func NewGitBlobstoreWithIdentity(gitDir, ref string, identity *git.Identity) (*GitBlobstore, error) {
	return NewGitBlobstoreWithOptions(gitDir, ref, GitBlobstoreOptions{Identity: identity})
}

// GitBlobstoreOptions configures optional behaviors of GitBlobstore.
type GitBlobstoreOptions struct {
	// Identity, when non-nil, forces the author/committer identity for commits created by write paths.
	Identity *git.Identity
	// MaxPartSize enables chunked-object writes when non-zero.
	// Read paths always support chunked objects if encountered.
	MaxPartSize uint64
}

// NewGitBlobstoreWithOptions creates a GitBlobstore rooted at |gitDir| and |ref|.
func NewGitBlobstoreWithOptions(gitDir, ref string, opts GitBlobstoreOptions) (*GitBlobstore, error) {
	r, err := git.NewRunner(gitDir)
	if err != nil {
		return nil, err
	}
	return &GitBlobstore{
		gitDir:      gitDir,
		ref:         ref,
		runner:      r,
		api:         git.NewGitAPIImpl(r),
		identity:    opts.Identity,
		maxPartSize: opts.MaxPartSize,
	}, nil
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

	return gbs.openBlobOrDescriptorRange(ctx, commit, blobOID, sz, br)
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitReadCloser) Close() error               { return l.c.Close() }

func (gbs *GitBlobstore) openBlobOrDescriptorRange(ctx context.Context, commit git.OID, blobOID git.OID, blobSize int64, br BlobRange) (io.ReadCloser, uint64, string, error) {
	ver := commit.String()

	// Read the blob contents. If it's a descriptor, we'll parse it and stream across parts.
	rc, err := gbs.api.BlobReader(ctx, blobOID)
	if err != nil {
		return nil, 0, ver, err
	}
	defer func() {
		if rc != nil {
			_ = rc.Close()
		}
	}()

	// Read up to a bounded prefix to determine if it's a descriptor. If it looks like one,
	// read the full blob (descriptors are expected to be small).
	const peekN = 64 * 1024
	peek := make([]byte, 0, 256)
	buf := make([]byte, 256)
	for len(peek) < cap(peek) {
		n, rerr := rc.Read(buf[:min(cap(peek)-len(peek), len(buf))])
		if n > 0 {
			peek = append(peek, buf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return nil, 0, ver, rerr
		}
	}

	// Not a descriptor: stream inline blob with BlobRange slicing.
	if !gitbs.IsDescriptorPrefix(peek) {
		// Re-open for streaming the full inline blob. (Simpler than splicing peek+rest.)
		_ = rc.Close()
		rc = nil

		inlineRC, err := gbs.api.BlobReader(ctx, blobOID)
		if err != nil {
			return nil, 0, ver, err
		}
		return sliceInlineBlob(inlineRC, blobSize, br, ver)
	}

	// It's probably a descriptor. Read the full contents (bounded defensively).
	// TODO(gitblobstore): add a MaxDescriptorSize config; for now cap at 64KiB.
	descBytes := append([]byte(nil), peek...)
	for int64(len(descBytes)) < blobSize && len(descBytes) < peekN {
		n, rerr := rc.Read(buf)
		if n > 0 {
			descBytes = append(descBytes, buf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return nil, 0, ver, rerr
		}
	}
	if int64(len(descBytes)) < blobSize {
		if blobSize > peekN {
			return nil, 0, ver, fmt.Errorf("gitblobstore: descriptor too large (%d bytes, cap %d)", blobSize, peekN)
		}
		return nil, 0, ver, io.ErrUnexpectedEOF
	}

	desc, err := gitbs.ParseDescriptor(descBytes)
	if err != nil {
		// Treat malformed descriptors as corruption (hard error).
		return nil, 0, ver, err
	}

	total := int64(desc.TotalSize)
	start, end, err := gitbs.NormalizeRange(total, br.offset, br.length)
	if err != nil {
		return nil, uint64(desc.TotalSize), ver, err
	}
	slices, err := gitbs.SliceParts(desc.Parts, start, end)
	if err != nil {
		return nil, uint64(desc.TotalSize), ver, err
	}

	// Stream across part blobs.
	streamRC := &multiPartReadCloser{
		ctx:    ctx,
		api:    gbs.api,
		slices: slices,
	}
	// Close descriptor blob reader (not used past this point).
	_ = rc.Close()
	rc = nil
	return streamRC, uint64(desc.TotalSize), ver, nil
}

func sliceInlineBlob(rc io.ReadCloser, sz int64, br BlobRange, ver string) (io.ReadCloser, uint64, string, error) {
	// Implement BlobRange by slicing the streamed blob contents.
	if br.isAllRange() {
		return rc, uint64(sz), ver, nil
	}

	pos := br.positiveRange(sz)
	if pos.offset < 0 || pos.offset > sz {
		_ = rc.Close()
		return nil, uint64(sz), ver, fmt.Errorf("invalid BlobRange offset %d for blob of size %d", pos.offset, sz)
	}
	if pos.length < 0 {
		_ = rc.Close()
		return nil, uint64(sz), ver, fmt.Errorf("invalid BlobRange length %d", pos.length)
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
			return nil, uint64(sz), ver, err
		}
	}

	return &limitReadCloser{r: io.LimitReader(rc, pos.length), c: rc}, uint64(sz), ver, nil
}

type multiPartReadCloser struct {
	ctx context.Context
	api git.GitAPI

	slices []gitbs.PartSlice
	curIdx int

	curRC io.ReadCloser
	rem   int64
}

func (m *multiPartReadCloser) Read(p []byte) (int, error) {
	for {
		if m.curRC == nil {
			if m.curIdx >= len(m.slices) {
				return 0, io.EOF
			}
			s := m.slices[m.curIdx]
			rc, err := m.api.BlobReader(m.ctx, git.OID(s.OIDHex))
			if err != nil {
				return 0, err
			}
			// Skip within part.
			if s.Offset > 0 {
				if _, err := io.CopyN(io.Discard, rc, s.Offset); err != nil {
					_ = rc.Close()
					return 0, err
				}
			}
			m.curRC = rc
			m.rem = s.Length
		}

		if m.rem == 0 {
			_ = m.curRC.Close()
			m.curRC = nil
			m.curIdx++
			continue
		}

		toRead := len(p)
		if int64(toRead) > m.rem {
			toRead = int(m.rem)
		}
		n, err := m.curRC.Read(p[:toRead])
		if n > 0 {
			m.rem -= int64(n)
			return n, nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				// End of underlying part blob; if we still expected bytes, that's corruption.
				if m.rem > 0 {
					return 0, io.ErrUnexpectedEOF
				}
				_ = m.curRC.Close()
				m.curRC = nil
				m.curIdx++
				continue
			}
			return 0, err
		}
	}
}

func (m *multiPartReadCloser) Close() error {
	if m.curRC != nil {
		err := m.curRC.Close()
		m.curRC = nil
		return err
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (gbs *GitBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: put %s", key)

	// Hash the contents once. If we need to retry due to concurrent updates to |gbs.ref|,
	// we can reuse the resulting object OIDs without re-reading |reader|.
	writes, err := gbs.planPutWrites(ctx, key, totalSize, reader)
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

		newCommit, err := gbs.buildCommitWithWrites(ctx, parent, ok, writes, msg)
		if err != nil {
			return backoff.Permanent(err)
		}

		if !ok {
			// Create-only CAS: oldOID=all-zero requires the ref to not exist. This avoids
			// losing concurrent writes when multiple goroutines create the ref at once.
			const zeroOID = git.OID("0000000000000000000000000000000000000000")
			if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, zeroOID, msg); err != nil {
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

type treeWrite struct {
	path string
	oid  git.OID
}

func (gbs *GitBlobstore) buildCommitWithMessage(ctx context.Context, parent git.OID, hasParent bool, key string, blobOID git.OID, msg string) (git.OID, error) {
	return gbs.buildCommitWithWrites(ctx, parent, hasParent, []treeWrite{{path: key, oid: blobOID}}, msg)
}

func (gbs *GitBlobstore) buildCommitWithWrites(ctx context.Context, parent git.OID, hasParent bool, writes []treeWrite, msg string) (git.OID, error) {
	_, indexFile, cleanup, err := newTempIndex()
	if err != nil {
		return "", err
	}
	defer cleanup()

	if hasParent {
		if err := gbs.api.ReadTree(ctx, parent, indexFile); err != nil {
			return "", err
		}
	} else {
		if err := gbs.api.ReadTreeEmpty(ctx, indexFile); err != nil {
			return "", err
		}
	}

	// TODO(gitblobstore): Decide on a policy for file-vs-directory prefix conflicts when staging keys.
	// For example, staging "a" when "a/b" already exists in the tree/index (or vice-versa) can fail
	// with a git index error (path appears as both a file and directory). Today our NBS keyspace is
	// flat (e.g. "manifest", "<tableid>", "<tableid>.records"), so this should not occur. If we ever
	// namespace keys into directories, consider proactively removing conflicting paths from the index
	// before UpdateIndexCacheInfo so Put/CheckAndPut remain robust.
	sort.Slice(writes, func(i, j int) bool { return writes[i].path < writes[j].path })
	for _, w := range writes {
		if err := gbs.api.UpdateIndexCacheInfo(ctx, indexFile, "100644", w.oid, w.path); err != nil {
			return "", err
		}
	}

	treeOID, err := gbs.api.WriteTree(ctx, indexFile)
	if err != nil {
		return "", err
	}

	var parentPtr *git.OID
	if hasParent && parent != "" {
		p := parent
		parentPtr = &p
	}

	// Prefer git's default identity from env/config when not explicitly configured.
	commitOID, err := gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, gbs.identity)
	if err != nil && gbs.identity == nil && isMissingGitIdentityErr(err) {
		commitOID, err = gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, defaultGitBlobstoreIdentity())
	}
	if err != nil {
		return "", err
	}

	return commitOID, nil
}

func (gbs *GitBlobstore) planPutWrites(ctx context.Context, key string, totalSize int64, reader io.Reader) ([]treeWrite, error) {
	// Minimal policy: chunk only when explicitly enabled and |totalSize| exceeds MaxPartSize.
	if gbs.maxPartSize == 0 || totalSize <= 0 || uint64(totalSize) <= gbs.maxPartSize {
		blobOID, err := gbs.api.HashObject(ctx, reader)
		if err != nil {
			return nil, err
		}
		return []treeWrite{{path: key, oid: blobOID}}, nil
	}

	descOID, partOIDs, err := gbs.hashChunkedObject(ctx, reader)
	if err != nil {
		return nil, err
	}

	writes := make([]treeWrite, 0, 1+len(partOIDs))
	writes = append(writes, treeWrite{path: key, oid: descOID})
	for _, p := range partOIDs {
		ppath, err := gitbs.PartPath(p.String())
		if err != nil {
			return nil, err
		}
		writes = append(writes, treeWrite{path: ppath, oid: p})
	}
	return writes, nil
}

func (gbs *GitBlobstore) hashChunkedObject(ctx context.Context, reader io.Reader) (descOID git.OID, partOIDs []git.OID, err error) {
	max := int64(gbs.maxPartSize)
	if max <= 0 {
		return "", nil, fmt.Errorf("gitblobstore: invalid maxPartSize %d", gbs.maxPartSize)
	}

	parts, partOIDs, total, err := gbs.hashParts(ctx, reader)
	if err != nil {
		return "", nil, err
	}

	descBytes, err := gitbs.EncodeDescriptor(gitbs.Descriptor{TotalSize: total, Parts: parts})
	if err != nil {
		return "", nil, err
	}
	descOID, err = gbs.api.HashObject(ctx, bytes.NewReader(descBytes))
	if err != nil {
		return "", nil, err
	}
	return descOID, partOIDs, nil
}

func (gbs *GitBlobstore) hashParts(ctx context.Context, reader io.Reader) (parts []gitbs.PartRef, partOIDs []git.OID, total uint64, err error) {
	max := int64(gbs.maxPartSize)
	if max <= 0 {
		return nil, nil, 0, fmt.Errorf("gitblobstore: invalid maxPartSize %d", gbs.maxPartSize)
	}

	buf := make([]byte, max)
	for {
		n, rerr := io.ReadFull(reader, buf)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			if !errors.Is(rerr, io.ErrUnexpectedEOF) {
				return nil, nil, 0, rerr
			}
			// ErrUnexpectedEOF: process final short chunk and stop.
		}
		if n == 0 {
			break
		}
		partBytes := append([]byte(nil), buf[:n]...)
		oid, err := gbs.api.HashObject(ctx, bytes.NewReader(partBytes))
		if err != nil {
			return nil, nil, 0, err
		}
		partOIDs = append(partOIDs, oid)
		parts = append(parts, gitbs.PartRef{OIDHex: oid.String(), Size: uint64(n)})
		total += uint64(n)
		if errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
	}
	return parts, partOIDs, total, nil
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
	// Create a unique temp index file. This is intentionally *not* placed under GIT_DIR:
	// - some git dirs may be read-only or otherwise unsuitable for scratch files
	// - we don't want to leave temp files inside the repo on crashes
	//
	// Note: git will also create a sibling lock file (<index>.lock) during index writes.
	f, err := os.CreateTemp("", "dolt-gitblobstore-index-")
	if err != nil {
		return "", "", nil, err
	}
	indexFile = f.Name()
	_ = f.Close()
	dir = filepath.Dir(indexFile)
	cleanup = func() {
		_ = os.Remove(indexFile)
		_ = os.Remove(indexFile + ".lock")
	}
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
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	// Resolve current head and validate expectedVersion before consuming |reader|.
	parent, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return "", err
	}
	actualVersion := ""
	if ok {
		actualVersion = parent.String()
	}
	if expectedVersion != actualVersion {
		return "", CheckAndPutError{Key: key, ExpectedVersion: expectedVersion, ActualVersion: actualVersion}
	}

	msg := fmt.Sprintf("gitblobstore: checkandput %s", key)
	writes, err := gbs.planPutWrites(ctx, key, totalSize, reader)
	if err != nil {
		return "", err
	}

	newCommit, err := gbs.buildCommitWithWrites(ctx, parent, ok, writes, msg)
	if err != nil {
		return "", err
	}

	if ok {
		if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, parent, msg); err != nil {
			// If the ref changed, surface as a standard mismatch error.
			cur, ok2, err2 := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
			if err2 == nil && ok2 && cur != parent {
				return "", CheckAndPutError{Key: key, ExpectedVersion: expectedVersion, ActualVersion: cur.String()}
			}
			return "", err
		}
		return newCommit.String(), nil
	}

	// Create-only CAS: oldOID=all-zero requires the ref to not exist.
	const zeroOID = git.OID("0000000000000000000000000000000000000000")
	if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, zeroOID, msg); err != nil {
		cur, ok2, err2 := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err2 == nil && ok2 {
			return "", CheckAndPutError{Key: key, ExpectedVersion: expectedVersion, ActualVersion: cur.String()}
		}
		return "", err
	}
	return newCommit.String(), nil
}

func (gbs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	normSources := make([]string, len(sources))
	for i, src := range sources {
		src, err := normalizeGitTreePath(src)
		if err != nil {
			return "", err
		}
		normSources[i] = src
	}

	// Snapshot the current head for reading sources so we don't depend on the ref staying
	// stable while we stream the concatenated contents into a new blob object.
	snapshot, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return "", err
	}
	if !ok && len(normSources) > 0 {
		// If the ref doesn't exist, the store is missing/corrupt (there is no commit to
		// resolve source paths against).
		return "", &git.RefNotFoundError{Ref: gbs.ref}
	}

	msg := fmt.Sprintf("gitblobstore: concatenate %s (%d sources)", key, len(normSources))

	var writes []treeWrite
	if gbs.maxPartSize == 0 {
		blobOID, err := gbs.hashConcatenation(ctx, snapshot, ok, normSources)
		if err != nil {
			return "", err
		}
		writes = []treeWrite{{path: key, oid: blobOID}}
	} else {
		writes, err = gbs.planConcatenateWritesChunked(ctx, snapshot, ok, key, normSources)
		if err != nil {
			return "", err
		}
	}

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
		parent, hasParent, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}

		newCommit, err := gbs.buildCommitWithWrites(ctx, parent, hasParent, writes, msg)
		if err != nil {
			return backoff.Permanent(err)
		}

		if !hasParent {
			// Create-only CAS: oldOID=all-zero requires the ref to not exist. This avoids
			// losing concurrent writes when multiple goroutines create the ref at once.
			const zeroOID = git.OID("0000000000000000000000000000000000000000")
			if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, zeroOID, msg); err != nil {
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

func (gbs *GitBlobstore) hashConcatenation(ctx context.Context, commit git.OID, hasCommit bool, sources []string) (git.OID, error) {
	if len(sources) == 0 {
		return gbs.api.HashObject(ctx, bytes.NewReader(nil))
	}
	if !hasCommit {
		return "", &git.RefNotFoundError{Ref: gbs.ref}
	}

	pr, pw := io.Pipe()
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer func() {
			_ = pw.Close()
		}()

		for _, src := range sources {
			blobOID, err := gbs.api.ResolvePathBlob(ectx, commit, src)
			if err != nil {
				if git.IsPathNotFound(err) {
					_ = pw.CloseWithError(NotFound{Key: src})
					return NotFound{Key: src}
				}
				_ = pw.CloseWithError(err)
				return err
			}

			rc, err := gbs.api.BlobReader(ectx, blobOID)
			if err != nil {
				_ = pw.CloseWithError(err)
				return err
			}

			_, err = io.Copy(pw, rc)
			cerr := rc.Close()
			if err == nil {
				err = cerr
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				return err
			}
		}
		return nil
	})

	oid, err := gbs.api.HashObject(ectx, pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		if werr := eg.Wait(); werr != nil {
			return "", werr
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}

	_ = pr.Close()
	if err := eg.Wait(); err != nil {
		return "", err
	}
	return oid, nil
}

type resolvedConcatSource struct {
	inlineOID  git.OID
	inlineSize int64
	desc       *gitbs.Descriptor
}

func (gbs *GitBlobstore) resolveConcatSource(ctx context.Context, commit git.OID, path string) (resolvedConcatSource, error) {
	blobOID, err := gbs.api.ResolvePathBlob(ctx, commit, path)
	if err != nil {
		if git.IsPathNotFound(err) {
			return resolvedConcatSource{}, NotFound{Key: path}
		}
		return resolvedConcatSource{}, err
	}
	sz, err := gbs.api.BlobSize(ctx, blobOID)
	if err != nil {
		return resolvedConcatSource{}, err
	}

	// Peek enough bytes to detect descriptor prefix conservatively.
	rc, err := gbs.api.BlobReader(ctx, blobOID)
	if err != nil {
		return resolvedConcatSource{}, err
	}
	defer rc.Close()

	peek := make([]byte, 0, 64)
	buf := make([]byte, 64)
	for len(peek) < cap(peek) {
		n, rerr := rc.Read(buf[:min(cap(peek)-len(peek), len(buf))])
		if n > 0 {
			peek = append(peek, buf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return resolvedConcatSource{}, rerr
		}
	}

	if !gitbs.IsDescriptorPrefix(peek) {
		return resolvedConcatSource{inlineOID: blobOID, inlineSize: sz}, nil
	}

	// Descriptor: re-read whole descriptor blob (bounded).
	// TODO(gitblobstore): configurable MaxDescriptorSize.
	const maxDesc = int64(64 * 1024)
	if sz > maxDesc {
		return resolvedConcatSource{}, fmt.Errorf("gitblobstore: descriptor too large (%d bytes, cap %d)", sz, maxDesc)
	}
	_ = rc.Close()

	rc2, err := gbs.api.BlobReader(ctx, blobOID)
	if err != nil {
		return resolvedConcatSource{}, err
	}
	defer rc2.Close()

	descBytes, err := io.ReadAll(rc2)
	if err != nil {
		return resolvedConcatSource{}, err
	}
	desc, err := gitbs.ParseDescriptor(descBytes)
	if err != nil {
		return resolvedConcatSource{}, err
	}
	return resolvedConcatSource{desc: &desc}, nil
}

func (gbs *GitBlobstore) planConcatenateWritesChunked(ctx context.Context, snapshot git.OID, hasSnapshot bool, key string, sources []string) ([]treeWrite, error) {
	if len(sources) == 0 {
		// Empty concatenation => empty object. Store inline.
		oid, err := gbs.api.HashObject(ctx, bytes.NewReader(nil))
		if err != nil {
			return nil, err
		}
		return []treeWrite{{path: key, oid: oid}}, nil
	}
	if !hasSnapshot {
		return nil, &git.RefNotFoundError{Ref: gbs.ref}
	}

	var (
		allParts   []gitbs.PartRef
		allPartOID = make(map[git.OID]struct{})
		total      uint64
	)

	for _, src := range sources {
		rs, err := gbs.resolveConcatSource(ctx, snapshot, src)
		if err != nil {
			return nil, err
		}

		var parts []gitbs.PartRef
		var oids []git.OID

		if rs.desc != nil {
			parts = rs.desc.Parts
			for _, p := range parts {
				oid := git.OID(p.OIDHex)
				if p.Size > gbs.maxPartSize {
					// Re-chunk oversized part.
					rc, err := gbs.api.BlobReader(ctx, oid)
					if err != nil {
						return nil, err
					}
					newParts, newOIDs, _, err := gbs.hashParts(ctx, rc)
					_ = rc.Close()
					if err != nil {
						return nil, err
					}
					allParts = append(allParts, newParts...)
					for _, no := range newOIDs {
						allPartOID[no] = struct{}{}
					}
					for _, np := range newParts {
						total += np.Size
					}
					continue
				}
				oids = append(oids, oid)
			}
		} else {
			// Inline.
			if rs.inlineSize < 0 {
				return nil, fmt.Errorf("gitblobstore: invalid inline size %d", rs.inlineSize)
			}
			if uint64(rs.inlineSize) > gbs.maxPartSize {
				// Re-chunk oversized inline blob.
				rc, err := gbs.api.BlobReader(ctx, rs.inlineOID)
				if err != nil {
					return nil, err
				}
				newParts, newOIDs, _, err := gbs.hashParts(ctx, rc)
				_ = rc.Close()
				if err != nil {
					return nil, err
				}
				parts = newParts
				oids = newOIDs
			} else {
				parts = []gitbs.PartRef{{OIDHex: rs.inlineOID.String(), Size: uint64(rs.inlineSize)}}
				oids = []git.OID{rs.inlineOID}
			}
		}

		allParts = append(allParts, parts...)
		for _, o := range oids {
			allPartOID[o] = struct{}{}
		}
		for _, p := range parts {
			total += p.Size
		}
	}

	descBytes, err := gitbs.EncodeDescriptor(gitbs.Descriptor{TotalSize: total, Parts: allParts})
	if err != nil {
		return nil, err
	}
	descOID, err := gbs.api.HashObject(ctx, bytes.NewReader(descBytes))
	if err != nil {
		return nil, err
	}

	writes := make([]treeWrite, 0, 1+len(allPartOID))
	writes = append(writes, treeWrite{path: key, oid: descOID})
	for oid := range allPartOID {
		ppath, err := gitbs.PartPath(oid.String())
		if err != nil {
			return nil, err
		}
		writes = append(writes, treeWrite{path: ppath, oid: oid})
	}
	return writes, nil
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
