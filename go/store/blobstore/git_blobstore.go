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
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

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
	_, _, err = gbs.api.ResolvePathObject(ctx, commit, key)
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
	commit, ver, err := gbs.resolveCommitForGet(ctx, key)
	if err != nil {
		return nil, 0, ver, err
	}

	oid, typ, ver, err := gbs.resolveObjectForGet(ctx, commit, key)
	if err != nil {
		return nil, 0, ver, err
	}

	switch typ {
	case "blob":
		sz, ver, err := gbs.resolveBlobSizeForGet(ctx, commit, oid)
		if err != nil {
			return nil, 0, ver, err
		}
		rc, err := gbs.api.BlobReader(ctx, oid)
		if err != nil {
			return nil, 0, ver, err
		}
		return sliceInlineBlob(rc, sz, br, ver)

	case "tree":
		return gbs.openChunkedTreeRange(ctx, commit, key, oid, br)

	default:
		return nil, 0, ver, fmt.Errorf("gitblobstore: unsupported object type %q for key %q", typ, key)
	}
}

func (gbs *GitBlobstore) resolveCommitForGet(ctx context.Context, key string) (commit git.OID, ver string, err error) {
	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return git.OID(""), "", err
	}
	if ok {
		return commit, commit.String(), nil
	}

	// If the ref doesn't exist, treat the manifest as missing (empty store),
	// but surface a hard error for other keys: the store itself is missing.
	if key == "manifest" {
		return git.OID(""), "", NotFound{Key: key}
	}
	return git.OID(""), "", &git.RefNotFoundError{Ref: gbs.ref}
}

func (gbs *GitBlobstore) resolveObjectForGet(ctx context.Context, commit git.OID, key string) (oid git.OID, typ string, ver string, err error) {
	oid, typ, err = gbs.api.ResolvePathObject(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return git.OID(""), "", commit.String(), NotFound{Key: key}
		}
		return git.OID(""), "", commit.String(), err
	}
	return oid, typ, commit.String(), nil
}

func (gbs *GitBlobstore) resolveBlobSizeForGet(ctx context.Context, commit git.OID, oid git.OID) (sz int64, ver string, err error) {
	sz, err = gbs.api.BlobSize(ctx, oid)
	if err != nil {
		return 0, commit.String(), err
	}
	return sz, commit.String(), nil
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitReadCloser) Close() error               { return l.c.Close() }

func (gbs *GitBlobstore) openChunkedTreeRange(ctx context.Context, commit git.OID, key string, treeOID git.OID, br BlobRange) (io.ReadCloser, uint64, string, error) {
	ver := commit.String()

	_ = treeOID // treeOID is informational; ListTree resolves by path.
	entries, err := gbs.api.ListTree(ctx, commit, key)
	if err != nil {
		return nil, 0, ver, err
	}
	parts, totalSize, err := gbs.validateAndSizeChunkedParts(ctx, entries)
	if err != nil {
		return nil, 0, ver, err
	}

	total := int64(totalSize)
	start, end, err := gitbs.NormalizeRange(total, br.offset, br.length)
	if err != nil {
		return nil, totalSize, ver, err
	}
	slices, err := gitbs.SliceParts(parts, start, end)
	if err != nil {
		return nil, totalSize, ver, err
	}

	// Stream across part blobs.
	streamRC := &multiPartReadCloser{
		ctx:    ctx,
		api:    gbs.api,
		slices: slices,
	}
	return streamRC, totalSize, ver, nil
}

func (gbs *GitBlobstore) validateAndSizeChunkedParts(ctx context.Context, entries []git.TreeEntry) ([]gitbs.PartRef, uint64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("gitblobstore: chunked tree has no parts")
	}

	width := len(entries[0].Name)
	// First pass: validate names + types, and determine width.
	if width < 4 {
		return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected at least 4 digits)", entries[0].Name)
	}

	parts := make([]gitbs.PartRef, 0, len(entries))
	var total uint64
	for i, e := range entries {
		if e.Type != "blob" {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part %q: expected blob, got %q", e.Name, e.Type)
		}
		if len(e.Name) != width {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected width %d)", e.Name, width)
		}
		n, err := strconv.Atoi(e.Name)
		if err != nil {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected digits): %w", e.Name, err)
		}
		if n != i+1 {
			want := fmt.Sprintf("%0*d", width, i+1)
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected %q)", e.Name, want)
		}
		if want := fmt.Sprintf("%0*d", width, n); want != e.Name {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected %q)", e.Name, want)
		}

		sz, err := gbs.api.BlobSize(ctx, e.OID)
		if err != nil {
			return nil, 0, err
		}
		if sz < 0 {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part size %d for %q", sz, e.Name)
		}
		if uint64(sz) > math.MaxUint64-total {
			return nil, 0, fmt.Errorf("gitblobstore: total size overflow")
		}
		total += uint64(sz)
		parts = append(parts, gitbs.PartRef{OIDHex: e.OID.String(), Size: uint64(sz)})
	}
	return parts, total, nil
}

func sliceInlineBlob(rc io.ReadCloser, sz int64, br BlobRange, ver string) (io.ReadCloser, uint64, string, error) {
	// Implement BlobRange by slicing the streamed blob contents.
	// TODO(gitblobstore): This streaming implementation is correct but may be slow for workloads
	// that do many small ranged reads (e.g. table index/footer reads). Consider caching/materializing
	// blobs to a local file (or using a batched git cat-file mode) to serve ranges efficiently.
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
		if err := m.ensureCurrent(); err != nil {
			return 0, err
		}
		if m.curRC == nil {
			return 0, io.EOF
		}

		if m.rem == 0 {
			_ = m.closeCurrentAndAdvance()
			continue
		}

		n, err := m.readCurrent(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
}

func (m *multiPartReadCloser) ensureCurrent() error {
	if m.curRC != nil {
		return nil
	}
	if m.curIdx >= len(m.slices) {
		return nil
	}
	s := m.slices[m.curIdx]
	rc, err := m.openSliceReader(s)
	if err != nil {
		return err
	}
	m.curRC = rc
	m.rem = s.Length
	return nil
}

func (m *multiPartReadCloser) openSliceReader(s gitbs.PartSlice) (io.ReadCloser, error) {
	rc, err := m.api.BlobReader(m.ctx, git.OID(s.OIDHex))
	if err != nil {
		return nil, err
	}
	if err := skipN(rc, s.Offset); err != nil {
		_ = rc.Close()
		return nil, err
	}
	return rc, nil
}

func (m *multiPartReadCloser) closeCurrentAndAdvance() error {
	if m.curRC != nil {
		err := m.curRC.Close()
		m.curRC = nil
		m.rem = 0
		m.curIdx++
		return err
	}
	m.curIdx++
	return nil
}

func (m *multiPartReadCloser) readCurrent(p []byte) (int, error) {
	toRead := len(p)
	if int64(toRead) > m.rem {
		toRead = int(m.rem)
	}

	n, err := m.curRC.Read(p[:toRead])
	if n > 0 {
		m.rem -= int64(n)
		return n, nil
	}
	if err == nil {
		return 0, nil
	}
	if errors.Is(err, io.EOF) {
		// End of underlying part blob; if we still expected bytes, that's corruption.
		if m.rem > 0 {
			return 0, io.ErrUnexpectedEOF
		}
		_ = m.closeCurrentAndAdvance()
		return 0, nil
	}
	return 0, err
}

func skipN(r io.Reader, n int64) error {
	if n <= 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, n)
	return err
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
	// Chunked-object support is landing in phases. Concatenate is the final piece
	// needed for NBS conjoin and is intentionally left unimplemented on this branch.
	//
	// Keep key validation for consistent error behavior.
	_, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}
	for _, src := range sources {
		if _, err := normalizeGitTreePath(src); err != nil {
			return "", err
		}
	}
	return "", git.ErrUnimplemented
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
