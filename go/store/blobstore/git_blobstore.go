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
	gitrebase "github.com/dolthub/dolt/go/store/blobstore/internal/gitrebase"
)

const gitblobstorePartNameWidth = 8 // "00000001"

type chunkPartRef struct {
	oidHex string
	size   uint64
}

type chunkPartSlice struct {
	oidHex string
	offset int64
	length int64
}

type treeWrite struct {
	path string
	oid  git.OID
}

type putPlan struct {
	writes []treeWrite
	// If true, the key should be represented as a tree (chunked parts under key/NNNNNNNN).
	chunked bool
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitReadCloser) Close() error               { return l.c.Close() }

type multiPartReadCloser struct {
	ctx context.Context
	api git.GitAPI

	slices []chunkPartSlice
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
			if err := m.closeCurrentAndAdvance(); err != nil {
				return 0, err
			}
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
	m.rem = s.length
	return nil
}

func (m *multiPartReadCloser) openSliceReader(s chunkPartSlice) (io.ReadCloser, error) {
	rc, err := m.api.BlobReader(m.ctx, git.OID(s.oidHex))
	if err != nil {
		return nil, err
	}
	if err := skipN(rc, s.offset); err != nil {
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

func (m *multiPartReadCloser) Close() error {
	if m.curRC != nil {
		err := m.curRC.Close()
		m.curRC = nil
		return err
	}
	return nil
}

type concatReadCloser struct {
	ctx   context.Context
	keys  []string
	open  func(ctx context.Context, key string) (io.ReadCloser, error)
	cur   int
	curRC io.ReadCloser
	done  bool
}

func (c *concatReadCloser) ensureCurrent() error {
	if c.done || c.curRC != nil {
		return nil
	}
	if c.cur >= len(c.keys) {
		c.done = true
		return nil
	}
	rc, err := c.open(c.ctx, c.keys[c.cur])
	if err != nil {
		return err
	}
	c.curRC = rc
	return nil
}

func (c *concatReadCloser) closeCurrentAndAdvance() error {
	if c.curRC != nil {
		err := c.curRC.Close()
		c.curRC = nil
		c.cur++
		return err
	}
	c.cur++
	return nil
}

func (c *concatReadCloser) Read(p []byte) (int, error) {
	for {
		if err := c.ensureCurrent(); err != nil {
			return 0, err
		}
		if c.curRC == nil {
			return 0, io.EOF
		}

		n, err := c.curRC.Read(p)
		if n > 0 {
			// Preserve data; defer advancement until next Read call.
			if err == io.EOF {
				_ = c.closeCurrentAndAdvance()
				return n, nil
			}
			return n, err
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			if cerr := c.closeCurrentAndAdvance(); cerr != nil {
				return 0, cerr
			}
			continue
		}
		return 0, err
	}
}

func (c *concatReadCloser) Close() error {
	c.done = true
	if c.curRC != nil {
		err := c.curRC.Close()
		c.curRC = nil
		return err
	}
	return nil
}

// GitBlobstore is a Blobstore implementation backed by a git repository's object
// database (bare repo or .git directory). It stores keys as paths within the tree
// of the commit referenced by a git ref (e.g. refs/dolt/data).
//
// This implementation is being developed in phases. Read paths were implemented first,
// then write paths were added incrementally.
type GitBlobstore struct {
	gitDir            string
	ref               string
	runner            *git.Runner
	api               git.GitAPI
	remoteManaged     bool
	remoteName        string
	remoteTrackingRef string
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
	// RemoteManaged enables remote-managed sync mode for this blobstore. When enabled, callers are expected
	// to configure a git remote (by name) in the underlying git repository; subsequent work will cause
	// Get/Put/CheckAndPut/Concatenate to fetch/merge/push automatically.
	RemoteManaged bool
	// RemoteName is the git remote name to use for remote-managed mode (e.g. "origin").
	// If empty and RemoteManaged is true, it defaults to "origin".
	RemoteName string
}

// NewGitBlobstoreWithOptions creates a GitBlobstore rooted at |gitDir| and |ref|.
func NewGitBlobstoreWithOptions(gitDir, ref string, opts GitBlobstoreOptions) (*GitBlobstore, error) {
	r, err := git.NewRunner(gitDir)
	if err != nil {
		return nil, err
	}

	remoteName := opts.RemoteName
	if opts.RemoteManaged && remoteName == "" {
		remoteName = "origin"
	}
	remoteTrackingRef := ""
	if opts.RemoteManaged {
		remoteTrackingRef = DoltRemoteTrackingDataRef(remoteName)
	}

	return &GitBlobstore{
		gitDir:            gitDir,
		ref:               ref,
		runner:            r,
		api:               git.NewGitAPIImpl(r),
		remoteManaged:     opts.RemoteManaged,
		remoteName:        remoteName,
		remoteTrackingRef: remoteTrackingRef,
		identity:          opts.Identity,
		maxPartSize:       opts.MaxPartSize,
	}, nil
}

func (gbs *GitBlobstore) Path() string {
	return fmt.Sprintf("%s@%s", gbs.gitDir, gbs.ref)
}

func (gbs *GitBlobstore) validateRemoteManaged() error {
	if gbs.remoteName == "" || gbs.remoteTrackingRef == "" {
		return fmt.Errorf("gitblobstore: remote-managed mode misconfigured (remoteName=%q trackingRef=%q)", gbs.remoteName, gbs.remoteTrackingRef)
	}
	return nil
}

func (gbs *GitBlobstore) syncForRead(ctx context.Context) error {
	if !gbs.remoteManaged {
		return nil
	}
	if err := gbs.validateRemoteManaged(); err != nil {
		return err
	}

	// 1) Fetch remote ref into our remote-tracking ref.
	if err := gbs.api.FetchRef(ctx, gbs.remoteName, gbs.ref, gbs.remoteTrackingRef); err != nil {
		return err
	}

	// 2) Merge tracking into local ref.
	policy := gbs.casRetryPolicy(ctx)
	op := func() error {
		old, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}

		_, _, err = gitrebase.MergeRemoteTrackingIntoLocalRefWithOptions(ctx, gbs.api, gbs.ref, gbs.remoteTrackingRef, gitrebase.MergeOptions{
			Message:    "gitblobstore: sync read",
			Author:     gbs.identity,
			OnConflict: gitrebase.ConflictRemoteWins,
		})
		if err == nil {
			return nil
		}
		if ok && gbs.refAdvanced(ctx, old) {
			return err
		}
		return backoff.Permanent(err)
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func (gbs *GitBlobstore) remoteManagedWrite(ctx context.Context, key, msg string, build func(parent git.OID, ok bool) (git.OID, error)) (string, error) {
	if err := gbs.validateRemoteManaged(); err != nil {
		return "", err
	}

	policy := gbs.casRetryPolicy(ctx)

	var ver string
	op := func() error {
		// 1) Fetch remote state into local tracking ref.
		if err := gbs.api.FetchRef(ctx, gbs.remoteName, gbs.ref, gbs.remoteTrackingRef); err != nil {
			return err
		}
		remoteHead, okRemote, err := gbs.api.TryResolveRefCommit(ctx, gbs.remoteTrackingRef)
		if err != nil {
			return backoff.Permanent(err)
		}
		if !okRemote {
			return backoff.Permanent(&git.RefNotFoundError{Ref: gbs.remoteTrackingRef})
		}

		// 2) Merge remote-tracking into local ref (remote is source-of-truth on conflicts).
		oldLocal, okLocal, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}
		_, _, err = gitrebase.MergeRemoteTrackingIntoLocalRefWithOptions(ctx, gbs.api, gbs.ref, gbs.remoteTrackingRef, gitrebase.MergeOptions{
			Message:    "gitblobstore: sync write",
			Author:     gbs.identity,
			OnConflict: gitrebase.ConflictRemoteWins,
		})
		if err != nil {
			// If local moved concurrently, retry; otherwise surface the error.
			if okLocal && gbs.refAdvanced(ctx, oldLocal) {
				return err
			}
			return backoff.Permanent(err)
		}

		// 3) Apply this operation's changes on top of the merged local head.
		parent, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}
		newCommit, err := build(parent, ok)
		if err != nil {
			return backoff.Permanent(err)
		}
		if err := gbs.updateRefCASForWrite(ctx, parent, ok, newCommit, msg); err != nil {
			return err
		}

		// 4) Push local ref to remote with lease.
		if err := gbs.api.PushRefWithLease(ctx, gbs.remoteName, gbs.ref, gbs.ref, remoteHead); err != nil {
			return err
		}

		ver, err = gbs.resolveKeyVersionAtCommit(ctx, newCommit, key)
		if err != nil {
			return backoff.Permanent(err)
		}
		return nil
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}
	return ver, nil
}

func (gbs *GitBlobstore) putWithRemoteSync(ctx context.Context, key string, plan putPlan, msg string) (string, error) {
	if !gbs.remoteManaged {
		return gbs.putWithCASRetries(ctx, key, plan, msg)
	}
	return gbs.remoteManagedWrite(ctx, key, msg, func(parent git.OID, ok bool) (git.OID, error) {
		return gbs.buildCommitForKeyWrite(ctx, parent, ok, key, plan, msg)
	})
}

func (gbs *GitBlobstore) checkAndPutWithRemoteSync(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader, msg string) (string, error) {
	if !gbs.remoteManaged {
		return "", fmt.Errorf("gitblobstore: internal error: checkAndPutWithRemoteSync called when remoteManaged=false")
	}
	var cachedPlan *putPlan
	return gbs.remoteManagedWrite(ctx, key, msg, func(parent git.OID, ok bool) (git.OID, error) {
		actualKeyVersion, err := gbs.currentKeyVersion(ctx, parent, ok, key)
		if err != nil {
			return git.OID(""), err
		}
		if expectedVersion != actualKeyVersion {
			return git.OID(""), CheckAndPutError{Key: key, ExpectedVersion: expectedVersion, ActualVersion: actualKeyVersion}
		}
		if cachedPlan == nil {
			plan, err := gbs.planPutWrites(ctx, key, totalSize, reader)
			if err != nil {
				return git.OID(""), err
			}
			cachedPlan = &plan
		}
		return gbs.buildCommitForKeyWrite(ctx, parent, ok, key, *cachedPlan, msg)
	})
}

func (gbs *GitBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return false, err
	}
	if err := gbs.syncForRead(ctx); err != nil {
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
	if err := gbs.syncForRead(ctx); err != nil {
		return nil, 0, "", err
	}
	commit, err := gbs.resolveCommitForGet(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}

	oid, typ, err := gbs.resolveObjectForGet(ctx, commit, key)
	if err != nil {
		return nil, 0, "", err
	}

	switch typ {
	case git.ObjectTypeBlob:
		sz, ver, err := gbs.resolveBlobSizeForGet(ctx, commit, oid)
		if err != nil {
			return nil, 0, ver, err
		}
		rc, err := gbs.api.BlobReader(ctx, oid)
		if err != nil {
			return nil, 0, ver, err
		}
		// Per-key version: blob object id.
		return sliceInlineBlob(rc, sz, br, oid.String())

	case git.ObjectTypeTree:
		// Per-key version: tree object id at this key.
		rc, sz, _, err := gbs.openChunkedTreeRange(ctx, commit, key, br)
		return rc, sz, oid.String(), err

	default:
		return nil, 0, "", fmt.Errorf("gitblobstore: unsupported object type %q for key %q", typ, key)
	}
}

func (gbs *GitBlobstore) openChunkedTreeRange(ctx context.Context, commit git.OID, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	ver := commit.String()

	entries, err := gbs.api.ListTree(ctx, commit, key)
	if err != nil {
		return nil, 0, ver, err
	}
	parts, totalSize, err := gbs.validateAndSizeChunkedParts(ctx, entries)
	if err != nil {
		return nil, 0, ver, err
	}

	total := int64(totalSize)
	start, end, err := normalizeRange(total, br.offset, br.length)
	if err != nil {
		return nil, totalSize, ver, err
	}
	slices, err := sliceChunkParts(parts, start, end)
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

func (gbs *GitBlobstore) validateAndSizeChunkedParts(ctx context.Context, entries []git.TreeEntry) ([]chunkPartRef, uint64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("gitblobstore: chunked tree has no parts")
	}

	width := len(entries[0].Name)
	// First pass: validate names + types, and determine width.
	if width < 4 {
		return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected at least 4 digits)", entries[0].Name)
	}

	parts := make([]chunkPartRef, 0, len(entries))
	var total uint64
	for i, e := range entries {
		if e.Type != git.ObjectTypeBlob {
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
		parts = append(parts, chunkPartRef{oidHex: e.OID.String(), size: uint64(sz)})
	}
	return parts, total, nil
}

func (gbs *GitBlobstore) resolveCommitForGet(ctx context.Context, key string) (commit git.OID, err error) {
	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return git.OID(""), err
	}
	if ok {
		return commit, nil
	}

	// If the ref doesn't exist, treat the manifest as missing (empty store),
	// but surface a hard error for other keys: the store itself is missing.
	if key == "manifest" {
		return git.OID(""), NotFound{Key: key}
	}
	return git.OID(""), &git.RefNotFoundError{Ref: gbs.ref}
}

func (gbs *GitBlobstore) resolveObjectForGet(ctx context.Context, commit git.OID, key string) (oid git.OID, typ git.ObjectType, err error) {
	oid, typ, err = gbs.api.ResolvePathObject(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return git.OID(""), git.ObjectTypeUnknown, NotFound{Key: key}
		}
		return git.OID(""), git.ObjectTypeUnknown, err
	}
	return oid, typ, nil
}

func (gbs *GitBlobstore) resolveBlobSizeForGet(ctx context.Context, commit git.OID, oid git.OID) (sz int64, ver string, err error) {
	sz, err = gbs.api.BlobSize(ctx, oid)
	if err != nil {
		return 0, commit.String(), err
	}
	return sz, commit.String(), nil
}

func (gbs *GitBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	if gbs.remoteManaged {
		// Ensure the idempotent "key exists" fast-path observes remote state.
		if err := gbs.syncForRead(ctx); err != nil {
			return "", err
		}
	}

	// Many NBS/table-file writes are content-addressed: if the key already exists, callers
	// assume it refers to the same bytes and treat the operation as idempotent.
	//
	// GitBlobstore enforces that assumption by fast-succeeding when a non-manifest key
	// already exists: it returns the existing per-key version and does not overwrite the
	// key (and does not consume |reader|).
	//
	// The manifest is the main exception (it is mutable and updated via CheckAndPut).
	if ver, ok, err := gbs.tryFastSucceedPutIfKeyExists(ctx, key); err != nil {
		return "", err
	} else if ok {
		return ver, nil
	}

	msg := fmt.Sprintf("gitblobstore: put %s", key)

	// Hash the contents once. If we need to retry due to concurrent updates to |gbs.ref|,
	// we can reuse the resulting object OIDs without re-reading |reader|.
	plan, err := gbs.planPutWrites(ctx, key, totalSize, reader)
	if err != nil {
		return "", err
	}
	if gbs.remoteManaged {
		return gbs.putWithRemoteSync(ctx, key, plan, msg)
	}
	return gbs.putWithCASRetries(ctx, key, plan, msg)
}

func (gbs *GitBlobstore) planPutWrites(ctx context.Context, key string, totalSize int64, reader io.Reader) (putPlan, error) {
	// Minimal policy: chunk only when explicitly enabled and |totalSize| exceeds MaxPartSize.
	if gbs.maxPartSize == 0 || totalSize <= 0 || uint64(totalSize) <= gbs.maxPartSize {
		blobOID, err := gbs.api.HashObject(ctx, reader)
		if err != nil {
			return putPlan{}, err
		}
		return putPlan{writes: []treeWrite{{path: key, oid: blobOID}}}, nil
	}

	partOIDs, err := gbs.hashChunkedParts(ctx, reader)
	if err != nil {
		return putPlan{}, err
	}

	writes := make([]treeWrite, 0, len(partOIDs))
	for i, p := range partOIDs {
		partName := fmt.Sprintf("%0*d", gitblobstorePartNameWidth, i+1)
		writes = append(writes, treeWrite{path: key + "/" + partName, oid: p})
	}
	return putPlan{writes: writes, chunked: true}, nil
}

func (gbs *GitBlobstore) hashChunkedParts(ctx context.Context, reader io.Reader) (partOIDs []git.OID, err error) {
	max := int64(gbs.maxPartSize)
	if max <= 0 {
		return nil, fmt.Errorf("gitblobstore: invalid maxPartSize %d", gbs.maxPartSize)
	}

	_, partOIDs, _, err = gbs.hashParts(ctx, reader)
	if err != nil {
		return nil, err
	}
	return partOIDs, nil
}

func (gbs *GitBlobstore) hashParts(ctx context.Context, reader io.Reader) (parts []chunkPartRef, partOIDs []git.OID, total uint64, err error) {
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
		parts = append(parts, chunkPartRef{oidHex: oid.String(), size: uint64(n)})
		total += uint64(n)
		if errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
	}
	return parts, partOIDs, total, nil
}

func (gbs *GitBlobstore) putWithCASRetries(ctx context.Context, key string, plan putPlan, msg string) (string, error) {
	// Make Put resilient to concurrent writers updating unrelated keys by using a CAS loop
	// under the hood. This matches typical object-store semantics more closely than an
	// unconditional ref update (which could clobber other keys).
	policy := gbs.casRetryPolicy(ctx)

	var ver string
	op := func() error {
		parent, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}

		newCommit, err := gbs.buildCommitForKeyWrite(ctx, parent, ok, key, plan, msg)
		if err != nil {
			return backoff.Permanent(err)
		}

		if err := gbs.updateRefCASForWrite(ctx, parent, ok, newCommit, msg); err != nil {
			return err
		}

		ver, err = gbs.resolveKeyVersionAtCommit(ctx, newCommit, key)
		if err != nil {
			return backoff.Permanent(err)
		}
		return nil
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}
	return ver, nil
}

func (gbs *GitBlobstore) casRetryPolicy(ctx context.Context) backoff.BackOff {
	const maxRetries = 31 // 32 total attempts (initial + retries)
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 5 * time.Millisecond
	bo.Multiplier = 2
	bo.MaxInterval = 320 * time.Millisecond
	bo.RandomizationFactor = 0 // deterministic; can add jitter later if needed
	bo.Reset()
	return backoff.WithContext(backoff.WithMaxRetries(bo, maxRetries), ctx)
}

func (gbs *GitBlobstore) buildCommitForKeyWrite(ctx context.Context, parent git.OID, hasParent bool, key string, plan putPlan, msg string) (git.OID, error) {
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

	if hasParent {
		if err := gbs.removeKeyConflictsFromIndex(ctx, parent, indexFile, key, plan.chunked); err != nil {
			return "", err
		}
	}

	sort.Slice(plan.writes, func(i, j int) bool { return plan.writes[i].path < plan.writes[j].path })
	for _, w := range plan.writes {
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

	commitOID, err := gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, gbs.identity)
	if err != nil && gbs.identity == nil && isMissingGitIdentityErr(err) {
		commitOID, err = gbs.api.CommitTree(ctx, treeOID, parentPtr, msg, defaultGitBlobstoreIdentity())
	}
	if err != nil {
		return "", err
	}
	return commitOID, nil
}

func (gbs *GitBlobstore) removeKeyConflictsFromIndex(ctx context.Context, parent git.OID, indexFile string, key string, newIsChunked bool) error {
	_, typ, err := gbs.api.ResolvePathObject(ctx, parent, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return nil
		}
		return err
	}

	switch typ {
	case git.ObjectTypeBlob:
		if newIsChunked {
			// blob -> tree: must remove the file entry at <key>
			return gbs.api.RemoveIndexPaths(ctx, indexFile, []string{key})
		}
		return nil

	case git.ObjectTypeTree:
		// tree -> blob OR tree overwrite: remove old child entries under <key>/...
		entries, err := gbs.api.ListTree(ctx, parent, key)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		paths := make([]string, 0, len(entries))
		for _, e := range entries {
			paths = append(paths, key+"/"+e.Name)
		}
		return gbs.api.RemoveIndexPaths(ctx, indexFile, paths)

	default:
		return fmt.Errorf("gitblobstore: unsupported existing object type %q at key %q", typ, key)
	}
}

func (gbs *GitBlobstore) updateRefCASForWrite(ctx context.Context, parent git.OID, haveParent bool, newCommit git.OID, msg string) error {
	if !haveParent {
		// Create-only CAS: oldOID=all-zero requires the ref to not exist. This avoids
		// losing concurrent writes when multiple goroutines create the ref at once.
		const zeroOID = git.OID("0000000000000000000000000000000000000000")
		if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, zeroOID, msg); err != nil {
			if gbs.refAdvanced(ctx, parent) {
				return err
			}
			return backoff.Permanent(err)
		}
		return nil
	}

	if err := gbs.api.UpdateRefCAS(ctx, gbs.ref, newCommit, parent, msg); err != nil {
		// If the ref changed since we read |parent|, retry on the new head. Otherwise
		// surface the error (e.g. permissions, corruption).
		if gbs.refAdvanced(ctx, parent) {
			return err
		}
		return backoff.Permanent(err)
	}
	return nil
}

func (gbs *GitBlobstore) refAdvanced(ctx context.Context, old git.OID) bool {
	if ctx.Err() != nil {
		return false
	}
	cur, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	return err == nil && ok && cur != old
}

func (gbs *GitBlobstore) resolveKeyVersionAtCommit(ctx context.Context, commit git.OID, key string) (string, error) {
	oid, _, err := gbs.api.ResolvePathObject(ctx, commit, key)
	if err != nil {
		return "", err
	}
	return oid.String(), nil
}

func (gbs *GitBlobstore) tryFastSucceedPutIfKeyExists(ctx context.Context, key string) (ver string, ok bool, err error) {
	if key == "manifest" {
		return "", false, nil
	}

	commit, haveCommit, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return "", false, err
	}
	if !haveCommit {
		return "", false, nil
	}

	oid, _, err := gbs.api.ResolvePathObject(ctx, commit, key)
	if err == nil {
		// Per-key version: existing object id.
		return oid.String(), true, nil
	}
	if git.IsPathNotFound(err) {
		return "", false, nil
	}
	return "", false, err
}

func (gbs *GitBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: checkandput %s", key)

	if gbs.remoteManaged {
		return gbs.checkAndPutWithRemoteSync(ctx, expectedVersion, key, totalSize, reader, msg)
	}

	policy := gbs.casRetryPolicy(ctx)

	var newKeyVersion string
	var cachedPlan *putPlan
	op := func() error {
		parent, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
		if err != nil {
			return backoff.Permanent(err)
		}

		actualKeyVersion, err := gbs.currentKeyVersion(ctx, parent, ok, key)
		if err != nil {
			return backoff.Permanent(err)
		}
		if expectedVersion != actualKeyVersion {
			return backoff.Permanent(CheckAndPutError{Key: key, ExpectedVersion: expectedVersion, ActualVersion: actualKeyVersion})
		}

		// Only hash/consume the reader once we know the expectedVersion matches.
		// If we need to retry due to unrelated ref advances, reuse the cached plan so we
		// don't re-read |reader| (which may not be rewindable).
		if cachedPlan == nil {
			plan, err := gbs.planPutWrites(ctx, key, totalSize, reader)
			if err != nil {
				return backoff.Permanent(err)
			}
			cachedPlan = &plan
		}

		newCommit, err := gbs.buildCommitForKeyWrite(ctx, parent, ok, key, *cachedPlan, msg)
		if err != nil {
			return backoff.Permanent(err)
		}

		if err := gbs.updateRefCASForWrite(ctx, parent, ok, newCommit, msg); err != nil {
			return err
		}

		ver, err := gbs.resolveKeyVersionAtCommit(ctx, newCommit, key)
		if err != nil {
			return backoff.Permanent(err)
		}
		newKeyVersion = ver
		return nil
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}

	return newKeyVersion, nil
}

func (gbs *GitBlobstore) currentKeyVersion(ctx context.Context, commit git.OID, haveCommit bool, key string) (string, error) {
	if !haveCommit {
		// Ref missing => empty store => key missing.
		return "", nil
	}
	oid, _, err := gbs.api.ResolvePathObject(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return oid.String(), nil
}

func (gbs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	// Keep key validation for consistent error behavior.
	var err error
	key, err = normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}
	if gbs.remoteManaged {
		if err := gbs.syncForRead(ctx); err != nil {
			return "", err
		}
	}
	if len(sources) == 0 {
		return "", fmt.Errorf("gitblobstore: concatenate requires at least one source")
	}
	normSources := make([]string, 0, len(sources))
	for _, src := range sources {
		norm, err := normalizeGitTreePath(src)
		if err != nil {
			return "", err
		}
		normSources = append(normSources, norm)
	}
	sources = normSources

	// For non-manifest keys, match Put's behavior: if the key already exists, succeed without overwriting.
	if ver, ok, err := gbs.tryFastSucceedPutIfKeyExists(ctx, key); err != nil {
		return "", err
	} else if ok {
		return ver, nil
	}

	// Resolve a snapshot commit for the sources.
	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.ref)
	if err != nil {
		return "", err
	}
	if !ok {
		// Consistent with Get: empty store => manifest missing, other keys => ref missing.
		if key == "manifest" {
			return "", NotFound{Key: key}
		}
		return "", &git.RefNotFoundError{Ref: gbs.ref}
	}

	totalSz, err := gbs.totalSizeAtCommit(ctx, commit, sources)
	if err != nil {
		return "", err
	}

	rc := &concatReadCloser{
		ctx:  ctx,
		keys: sources,
		open: func(ctx context.Context, k string) (io.ReadCloser, error) {
			return gbs.openReaderAtCommit(ctx, commit, k)
		},
	}
	defer rc.Close()

	plan, err := gbs.planPutWrites(ctx, key, totalSz, rc)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: concatenate %s", key)
	if gbs.remoteManaged {
		return gbs.putWithRemoteSync(ctx, key, plan, msg)
	}
	return gbs.putWithCASRetries(ctx, key, plan, msg)
}

func (gbs *GitBlobstore) openReaderAtCommit(ctx context.Context, commit git.OID, key string) (io.ReadCloser, error) {
	oid, typ, err := gbs.resolveObjectForGet(ctx, commit, key)
	if err != nil {
		return nil, err
	}
	switch typ {
	case git.ObjectTypeBlob:
		return gbs.api.BlobReader(ctx, oid)
	case git.ObjectTypeTree:
		rc, _, _, err := gbs.openChunkedTreeRange(ctx, commit, key, AllRange)
		if err != nil {
			// Defensive: resolveObjectForGet succeeded, but keep NotFound mapping consistent.
			var pnf *git.PathNotFoundError
			if errors.As(err, &pnf) {
				return nil, NotFound{Key: key}
			}
			return nil, err
		}
		return rc, nil
	default:
		return nil, fmt.Errorf("gitblobstore: unsupported object type %q for key %q", typ, key)
	}
}

// sizeAtCommit returns the byte size of |key| as of |commit|.
// It supports both inline blobs and the chunked-tree representation used by GitBlobstore.
// If |key| is missing at |commit|, it returns NotFound{Key: key}.
func (gbs *GitBlobstore) sizeAtCommit(ctx context.Context, commit git.OID, key string) (uint64, error) {
	oid, typ, err := gbs.api.ResolvePathObject(ctx, commit, key)
	if err != nil {
		if git.IsPathNotFound(err) {
			return 0, NotFound{Key: key}
		}
		return 0, err
	}

	switch typ {
	case git.ObjectTypeBlob:
		sz, err := gbs.api.BlobSize(ctx, oid)
		if err != nil {
			return 0, err
		}
		if sz < 0 {
			return 0, fmt.Errorf("gitblobstore: invalid blob size %d for key %q", sz, key)
		}
		return uint64(sz), nil

	case git.ObjectTypeTree:
		entries, err := gbs.api.ListTree(ctx, commit, key)
		if err != nil {
			if git.IsPathNotFound(err) {
				return 0, NotFound{Key: key}
			}
			return 0, err
		}
		_, total, err := gbs.validateAndSizeChunkedParts(ctx, entries)
		return total, err

	default:
		return 0, fmt.Errorf("gitblobstore: unsupported object type %q for key %q", typ, key)
	}
}

// totalSizeAtCommit sums the sizes of |sources| at |commit| and returns the total as int64.
// Returns an error on overflow or if any source is missing.
func (gbs *GitBlobstore) totalSizeAtCommit(ctx context.Context, commit git.OID, sources []string) (int64, error) {
	var total uint64
	for _, src := range sources {
		sz, err := gbs.sizeAtCommit(ctx, commit, src)
		if err != nil {
			return 0, err
		}
		if sz > math.MaxUint64-total {
			return 0, fmt.Errorf("gitblobstore: concatenated size overflow")
		}
		total += sz
	}
	if total > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("gitblobstore: concatenated size %d overflows int64", total)
	}
	return int64(total), nil
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

func skipN(r io.Reader, n int64) error {
	if n <= 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, n)
	return err
}

func normalizeRange(total int64, offset int64, length int64) (start, end int64, err error) {
	if total < 0 {
		return 0, 0, fmt.Errorf("invalid total size %d", total)
	}
	if length < 0 {
		return 0, 0, fmt.Errorf("invalid length %d", length)
	}
	start = offset
	if start < 0 {
		start = total + start
	}
	if start < 0 || start > total {
		return 0, 0, fmt.Errorf("invalid offset %d for total size %d", offset, total)
	}
	if length == 0 {
		end = total
	} else {
		end = start + length
		if end < start {
			return 0, 0, fmt.Errorf("range overflow")
		}
		if end > total {
			end = total
		}
	}
	return start, end, nil
}

func sliceChunkParts(parts []chunkPartRef, start, end int64) ([]chunkPartSlice, error) {
	if start < 0 || end < 0 || end < start {
		return nil, fmt.Errorf("invalid start/end: %d/%d", start, end)
	}
	if start == end {
		return nil, nil
	}

	var (
		out []chunkPartSlice
		pos int64
	)

	for _, p := range parts {
		if p.size == 0 {
			return nil, fmt.Errorf("invalid part size 0")
		}
		partStart := pos
		partEnd := pos + int64(p.size)
		if partEnd < partStart {
			return nil, fmt.Errorf("part size overflow")
		}

		if end <= partStart {
			break
		}
		if start >= partEnd {
			pos = partEnd
			continue
		}

		s := start
		if s < partStart {
			s = partStart
		}
		e := end
		if e > partEnd {
			e = partEnd
		}
		if e > s {
			out = append(out, chunkPartSlice{
				oidHex: p.oidHex,
				offset: s - partStart,
				length: e - s,
			})
		}
		pos = partEnd
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("range [%d,%d) not covered by parts", start, end)
	}
	var covered int64
	for _, s := range out {
		covered += s.length
	}
	if covered != (end - start) {
		return nil, fmt.Errorf("range [%d,%d) not fully covered by parts", start, end)
	}
	return out, nil
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
