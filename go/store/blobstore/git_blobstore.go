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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

const gitblobstorePartNameWidth = 4 // "0001"

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
	// If true, the key should be represented as a tree (chunked parts under key/NNNN).
	chunked bool
}

// pendingWrite holds a deferred non-manifest write that will be flushed
// in a single commit+push when CheckAndPut("manifest") is called.
type pendingWrite struct {
	key  string
	plan putPlan
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
// Cache semantics:
//   - GitBlobstore maintains an in-memory path->(oid,type) cache built from fetched heads.
//   - The cache is monotonic/merge-only: once a non-manifest key is cached, it is treated
//     as immutable and its mapping is not overwritten by later fetches.
//   - The manifest is the exception: it is mutable and may be updated on fetch/merge.
type GitBlobstore struct {
	gitDir            string
	remoteRef         string
	localRef          string
	runner            *git.Runner
	api               git.GitAPI
	remoteName        string
	remoteTrackingRef string
	mu                sync.Mutex
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

	// pendingWrites accumulates non-manifest writes that will be flushed in a single
	// commit+push when CheckAndPut("manifest") is called. This avoids per-key
	// fetch/commit/push cycles for content-addressed (immutable) table file blobs.
	// Guarded by gbs.mu.
	pendingWrites []pendingWrite

	// cacheMu guards all cache fields below.
	cacheMu sync.RWMutex
	// cacheHead is the last commit OID whose tree we merged into the cache.
	// An empty value means "no cache merged yet".
	cacheHead git.OID
	// cacheObjects maps full tree paths to their object OID and type.
	cacheObjects map[string]cachedGitObject
	// cacheChildren maps a directory path to its immediate children entries. The
	// entries' Name field is the base name (not a full path).
	cacheChildren map[string][]git.TreeEntry
}

var _ Blobstore = (*GitBlobstore)(nil)

type cachedGitObject struct {
	oid git.OID
	typ git.ObjectType
}

func (gbs *GitBlobstore) cacheGetObject(path string) (cachedGitObject, bool) {
	gbs.cacheMu.RLock()
	obj, ok := gbs.cacheObjects[path]
	gbs.cacheMu.RUnlock()
	return obj, ok
}

func (gbs *GitBlobstore) cacheListChildren(dir string) ([]git.TreeEntry, bool) {
	gbs.cacheMu.RLock()
	ents, ok := gbs.cacheChildren[dir]
	gbs.cacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	// Return a copy to prevent callers from mutating internal cache.
	cp := append([]git.TreeEntry(nil), ents...)
	return cp, true
}

// NewGitBlobstore creates a new GitBlobstore rooted at |gitDir| and |ref|.
// |gitDir| should point at a bare repo directory or a .git directory.
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
	// RemoteName is the git remote name to use for remote-managed mode (e.g. "origin").
	// If empty, it defaults to "origin".
	RemoteName string
}

// NewGitBlobstoreWithOptions creates a GitBlobstore rooted at |gitDir| and |ref|.
func NewGitBlobstoreWithOptions(gitDir, ref string, opts GitBlobstoreOptions) (*GitBlobstore, error) {
	r, err := git.NewRunner(gitDir)
	if err != nil {
		return nil, err
	}

	remoteName := opts.RemoteName
	if remoteName == "" {
		remoteName = "origin"
	}
	remoteRef := ref
	instanceID := uuid.NewString()
	remoteTrackingRef := RemoteTrackingRef(remoteName, remoteRef, instanceID)
	localRef := OwnedLocalRef(remoteName, remoteRef, instanceID)

	return &GitBlobstore{
		gitDir:            gitDir,
		remoteRef:         remoteRef,
		localRef:          localRef,
		runner:            r,
		api:               git.NewGitAPIImpl(r),
		remoteName:        remoteName,
		remoteTrackingRef: remoteTrackingRef,
		identity:          opts.Identity,
		maxPartSize:       opts.MaxPartSize,
		cacheObjects:      make(map[string]cachedGitObject),
		cacheChildren:     make(map[string][]git.TreeEntry),
	}, nil
}

func splitGitPathParentBase(fullPath string) (parent string, base string) {
	i := strings.LastIndexByte(fullPath, '/')
	if i < 0 {
		return "", fullPath
	}
	return fullPath[:i], fullPath[i+1:]
}

const gitblobstoreManifestKey = "manifest"

func (gbs *GitBlobstore) mergeCacheObjectLocked(path string, oid git.OID, typ git.ObjectType, overwrite bool) {
	if _, ok := gbs.cacheObjects[path]; !ok || overwrite {
		gbs.cacheObjects[path] = cachedGitObject{oid: oid, typ: typ}
	}
}

func (gbs *GitBlobstore) mergeCacheChildLocked(parent string, child git.TreeEntry, overwrite bool) (touched bool) {
	ents := gbs.cacheChildren[parent]
	for i := range ents {
		if ents[i].Name != child.Name {
			continue
		}
		if overwrite {
			ents[i] = child
			gbs.cacheChildren[parent] = ents
			return true
		}
		return false
	}
	gbs.cacheChildren[parent] = append(ents, child)
	return true
}

func (gbs *GitBlobstore) sortCacheChildrenLocked(dirs map[string]struct{}) {
	// Deterministic ordering for callers that require sorted names (e.g. chunk parts 0001..).
	for dir := range dirs {
		sort.Slice(gbs.cacheChildren[dir], func(i, j int) bool { return gbs.cacheChildren[dir][i].Name < gbs.cacheChildren[dir][j].Name })
	}
}

// mergeCacheFromHead additively merges the tree entries from the given head
// commit into the cache. Existing entries are never overwritten — the cache is
// append-only — except for the manifest, which must always reflect the latest
// remote state.
func (gbs *GitBlobstore) mergeCacheFromHead(ctx context.Context, head git.OID) error {
	if head == "" {
		return fmt.Errorf("gitblobstore: cannot merge cache for empty head")
	}

	gbs.cacheMu.RLock()
	if gbs.cacheHead == head {
		gbs.cacheMu.RUnlock()
		return nil
	}
	gbs.cacheMu.RUnlock()

	entries, err := gbs.api.ListTreeRecursive(ctx, head)
	if err != nil {
		return err
	}

	gbs.cacheMu.Lock()

	// Double-check under write lock for concurrent callers.
	if gbs.cacheHead == head {
		gbs.cacheMu.Unlock()
		return nil
	}

	// Defensive: allow zero-value GitBlobstore in tests; initialize once if nil.
	if gbs.cacheObjects == nil {
		gbs.cacheObjects = make(map[string]cachedGitObject)
	}
	if gbs.cacheChildren == nil {
		gbs.cacheChildren = make(map[string][]git.TreeEntry)
	}

	touchedDirs := make(map[string]struct{})

	for _, e := range entries {
		if e.Name == "" {
			continue
		}

		// The manifest is the only entry that changes content at the same
		// path, so it must always be overwritten to reflect the latest state.
		overwrite := e.Name == gitblobstoreManifestKey
		gbs.mergeCacheObjectLocked(e.Name, e.OID, e.Type, overwrite)

		// Merge parent -> child membership (ensure presence; overwrite manifest only).
		parent, base := splitGitPathParentBase(e.Name)
		if base == "" {
			continue
		}

		child := git.TreeEntry{Mode: e.Mode, Type: e.Type, OID: e.OID, Name: base}
		if gbs.mergeCacheChildLocked(parent, child, overwrite) {
			touchedDirs[parent] = struct{}{}
		}
	}

	gbs.sortCacheChildrenLocked(touchedDirs)

	gbs.cacheHead = head
	gbs.cacheMu.Unlock()
	return nil
}

func (gbs *GitBlobstore) Path() string {
	return fmt.Sprintf("%s@%s", gbs.gitDir, gbs.remoteRef)
}

func (gbs *GitBlobstore) validateRemoteManaged() error {
	if gbs.remoteName == "" || gbs.remoteRef == "" || gbs.remoteTrackingRef == "" || gbs.localRef == "" {
		return fmt.Errorf("gitblobstore: remote-managed mode misconfigured (remoteName=%q remoteRef=%q trackingRef=%q localRef=%q)", gbs.remoteName, gbs.remoteRef, gbs.remoteTrackingRef, gbs.localRef)
	}
	return nil
}

// CleanupOwnedLocalRef best-effort deletes this blobstore instance's UUID-owned local ref.
//
// This is an optional hygiene API: by default, UUID-owned local refs may accumulate in the
// repo over time. Callers that care about cleanup (e.g. tests) may invoke this explicitly.
func (gbs *GitBlobstore) CleanupOwnedLocalRef(ctx context.Context) error {
	gbs.mu.Lock()
	defer gbs.mu.Unlock()

	_, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.localRef)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	_, err = gbs.runner.Run(ctx, git.RunOptions{}, "update-ref", "-d", gbs.localRef)
	return err
}

func (gbs *GitBlobstore) syncForRead(ctx context.Context) error {
	if err := gbs.validateRemoteManaged(); err != nil {
		return err
	}
	gbs.mu.Lock()
	defer gbs.mu.Unlock()

	// 1) Fetch remote ref into our remote-tracking ref.
	if err := gbs.api.FetchRef(ctx, gbs.remoteName, gbs.remoteRef, gbs.remoteTrackingRef); err != nil {
		// An absent remote ref is treated as an empty store. This is required for NBS open
		// (manifest ParseIfExists) against a freshly-initialized remote.
		var rnf *git.RefNotFoundError
		if errors.As(err, &rnf) && rnf.Ref == gbs.remoteRef {
			return nil
		}
		return err
	}

	remoteHead, okRemote, err := gbs.api.TryResolveRefCommit(ctx, gbs.remoteTrackingRef)
	if err != nil {
		return err
	}
	if !okRemote {
		return &git.RefNotFoundError{Ref: gbs.remoteTrackingRef}
	}

	// 2) Force-set owned local ref to remote head (no merge; remote is source-of-truth).
	if err := gbs.api.UpdateRef(ctx, gbs.localRef, remoteHead, "gitblobstore: sync read"); err != nil {
		return err
	}

	// 3) Merge cache to reflect fetched contents.
	return gbs.mergeCacheFromHead(ctx, remoteHead)
}

type gitblobstoreFetchRefError struct {
	err error
}

func (e *gitblobstoreFetchRefError) Error() string { return e.err.Error() }
func (e *gitblobstoreFetchRefError) Unwrap() error { return e.err }

func (gbs *GitBlobstore) fetchAlignAndMergeForWrite(ctx context.Context) (remoteHead git.OID, ok bool, err error) {
	if err := gbs.api.FetchRef(ctx, gbs.remoteName, gbs.remoteRef, gbs.remoteTrackingRef); err != nil {
		// If the remote ref is missing, treat this as an empty store and bootstrap on write.
		// Note: there is no "empty ref" in Git; this means the ref is unborn (no commits yet).
		// Callers will see ok=false and parent=="" and will:
		// - build a root commit from an empty tree (no parent),
		// - create/update gbs.localRef to that commit, and
		// - push with an empty expected dst OID, which creates gbs.remoteRef on the remote.
		var rnf *git.RefNotFoundError
		if errors.As(err, &rnf) && rnf.Ref == gbs.remoteRef {
			return "", false, nil
		}
		return "", false, &gitblobstoreFetchRefError{err: err}
	}

	remoteHead, ok, err = gbs.api.TryResolveRefCommit(ctx, gbs.remoteTrackingRef)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, &git.RefNotFoundError{Ref: gbs.remoteTrackingRef}
	}

	// Force-set owned local ref to remote head (remote is source-of-truth).
	if err := gbs.api.UpdateRef(ctx, gbs.localRef, remoteHead, "gitblobstore: sync write"); err != nil {
		return "", false, err
	}

	// Merge cache to reflect fetched contents.
	if err := gbs.mergeCacheFromHead(ctx, remoteHead); err != nil {
		return "", false, err
	}

	return remoteHead, true, nil
}

func (gbs *GitBlobstore) remoteManagedWrite(ctx context.Context, key, msg string, build func(parent git.OID, ok bool) (git.OID, error)) (string, error) {
	if err := gbs.validateRemoteManaged(); err != nil {
		return "", err
	}
	gbs.mu.Lock()
	defer gbs.mu.Unlock()

	policy := gbs.casRetryPolicy(ctx)

	var ver string
	op := func() error {
		remoteHead, okRemote, err := gbs.fetchAlignAndMergeForWrite(ctx)
		if err != nil {
			var fe *gitblobstoreFetchRefError
			if errors.As(err, &fe) {
				return fe.err
			}
			return backoff.Permanent(err)
		}

		// Apply this operation's changes on top of the remote head (or empty store).
		newCommit, err := build(remoteHead, okRemote)
		if err != nil {
			return backoff.Permanent(err)
		}
		if err := gbs.api.UpdateRef(ctx, gbs.localRef, newCommit, msg); err != nil {
			return backoff.Permanent(err)
		}

		// Push local ref to remote with lease.
		if err := gbs.api.PushRefWithLease(ctx, gbs.remoteName, gbs.localRef, gbs.remoteRef, remoteHead); err != nil {
			return err
		}

		// Merge cache additively to reflect the new head after a successful push.
		if err := gbs.mergeCacheFromHead(ctx, newCommit); err != nil {
			return backoff.Permanent(err)
		}

		// Force-update the cache entry for the key we just wrote, since the
		// additive merge won't overwrite a stale entry from a previous commit.
		keyOID, keyType, err := gbs.api.ResolvePathObject(ctx, newCommit, key)
		if err != nil {
			return backoff.Permanent(err)
		}
		gbs.cacheMu.Lock()
		gbs.mergeCacheObjectLocked(key, keyOID, keyType, true)
		parent, base := splitGitPathParentBase(key)
		if base != "" {
			mode := "100644"
			if keyType == git.ObjectTypeTree {
				mode = "040000"
			}
			child := git.TreeEntry{Mode: mode, Type: keyType, OID: keyOID, Name: base}
			gbs.mergeCacheChildLocked(parent, child, true)
		}
		gbs.cacheMu.Unlock()

		ver = keyOID.String()
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
	return gbs.remoteManagedWrite(ctx, key, msg, func(remoteHead git.OID, ok bool) (git.OID, error) {
		return gbs.buildCommitForKeyWrite(ctx, remoteHead, ok, key, plan, msg, nil)
	})
}

func (gbs *GitBlobstore) checkAndPutWithRemoteSync(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader, msg string, extraWrites []pendingWrite) (string, error) {
	var cachedPlan *putPlan
	return gbs.remoteManagedWrite(ctx, key, msg, func(remoteHead git.OID, ok bool) (git.OID, error) {
		actualKeyVersion, err := gbs.currentKeyVersion(ctx, remoteHead, ok, key)
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
		return gbs.buildCommitForKeyWrite(ctx, remoteHead, ok, key, *cachedPlan, msg, extraWrites)
	})
}

func (gbs *GitBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return false, err
	}

	// For non-manifest content-addressed keys, check the cache first.
	// If found, skip the remote fetch (covers putDeferred and prior fetches).
	if key != gitblobstoreManifestKey {
		if _, ok := gbs.cacheGetObject(key); ok {
			return true, nil
		}
	}

	if err := gbs.syncForRead(ctx); err != nil {
		return false, err
	}
	_, ok := gbs.cacheGetObject(key)
	return ok, nil
}

func (gbs *GitBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return nil, 0, "", err
	}

	// For non-manifest content-addressed keys, try the cache first (covers
	// keys written via putDeferred or already fetched). Only fetch from
	// remote if the key isn't cached yet.
	if key != gitblobstoreManifestKey {
		if _, ok := gbs.cacheGetObject(key); ok {
			return gbs.getFromCache(ctx, key, br)
		}
	}

	if err := gbs.syncForRead(ctx); err != nil {
		return nil, 0, "", err
	}
	return gbs.getFromCache(ctx, key, br)
}

func (gbs *GitBlobstore) getFromCache(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	obj, ok := gbs.cacheGetObject(key)
	if !ok {
		return nil, 0, "", NotFound{Key: key}
	}

	switch obj.typ {
	case git.ObjectTypeBlob:
		sz, err := gbs.api.BlobSize(ctx, obj.oid)
		if err != nil {
			return nil, 0, "", err
		}
		rc, err := gbs.api.BlobReader(ctx, obj.oid)
		if err != nil {
			return nil, 0, "", err
		}
		// Per-key version: blob object id.
		return sliceInlineBlob(rc, sz, br, obj.oid.String())

	case git.ObjectTypeTree:
		// Per-key version: tree object id at this key.
		rc, sz, err := gbs.openChunkedTreeRange(ctx, key, br)
		return rc, sz, obj.oid.String(), err

	default:
		return nil, 0, "", fmt.Errorf("gitblobstore: unsupported object type %q for key %q", obj.typ, key)
	}
}

func (gbs *GitBlobstore) openChunkedTreeRange(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, error) {
	entries, ok := gbs.cacheListChildren(key)
	if !ok {
		return nil, 0, NotFound{Key: key}
	}
	parts, totalSize, err := gbs.validateAndSizeChunkedParts(ctx, entries)
	if err != nil {
		return nil, 0, err
	}

	total := int64(totalSize)
	start, end, err := normalizeRange(total, br.offset, br.length)
	if err != nil {
		return nil, totalSize, err
	}
	slices, err := sliceChunkParts(parts, start, end)
	if err != nil {
		return nil, totalSize, err
	}

	// Stream across part blobs.
	streamRC := &multiPartReadCloser{
		ctx:    ctx,
		api:    gbs.api,
		slices: slices,
	}
	return streamRC, totalSize, nil
}

func (gbs *GitBlobstore) validateAndSizeChunkedParts(ctx context.Context, entries []git.TreeEntry) ([]chunkPartRef, uint64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("gitblobstore: chunked tree has no parts")
	}

	// GitBlobstore chunked trees use fixed-width 4-digit part names: 0001, 0002, ...
	if len(entries[0].Name) != gitblobstorePartNameWidth {
		return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected width %d)", entries[0].Name, gitblobstorePartNameWidth)
	}

	parts := make([]chunkPartRef, 0, len(entries))
	var total uint64
	for i, e := range entries {
		if e.Type != git.ObjectTypeBlob {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part %q: expected blob, got %q", e.Name, e.Type)
		}
		if len(e.Name) != gitblobstorePartNameWidth {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected width %d)", e.Name, gitblobstorePartNameWidth)
		}
		n, err := strconv.Atoi(e.Name)
		if err != nil {
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected digits): %w", e.Name, err)
		}
		if n != i+1 {
			want := fmt.Sprintf("%0*d", gitblobstorePartNameWidth, i+1)
			return nil, 0, fmt.Errorf("gitblobstore: invalid part name %q (expected %q)", e.Name, want)
		}
		if want := fmt.Sprintf("%0*d", gitblobstorePartNameWidth, n); want != e.Name {
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

func (gbs *GitBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	// For non-manifest keys, skip remote sync entirely: check cache, hash locally,
	// and defer the write to be flushed in the next CheckAndPut("manifest").
	if key != gitblobstoreManifestKey {
		if obj, ok := gbs.cacheGetObject(key); ok {
			return obj.oid.String(), nil
		}
		plan, err := gbs.planPutWrites(ctx, key, totalSize, reader)
		if err != nil {
			return "", err
		}
		return gbs.enqueuePendingWrite(key, plan), nil
	}

	// Manifest key: fall through to existing remote-synced path.
	if err := gbs.syncForRead(ctx); err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: put %s", key)
	plan, err := gbs.planPutWrites(ctx, key, totalSize, reader)
	if err != nil {
		return "", err
	}
	return gbs.putWithRemoteSync(ctx, key, plan, msg)
}

// cacheUpdateForPlan updates the in-memory cache for a locally-hashed putPlan
// so that subsequent reads (e.g. Concatenate source resolution) can find the data.
func (gbs *GitBlobstore) cacheUpdateForPlan(key string, plan putPlan) {
	gbs.cacheMu.Lock()
	defer gbs.cacheMu.Unlock()

	if plan.chunked {
		// For chunked writes, cache each part blob and the parent directory's children.
		// Use the first part's OID as a placeholder tree OID for version consistency
		// between Put and idempotent re-Put. The actual tree OID is computed at commit time.
		placeholderOID := git.OID("")
		if len(plan.writes) > 0 {
			placeholderOID = plan.writes[0].oid
		}
		gbs.mergeCacheObjectLocked(key, placeholderOID, git.ObjectTypeTree, false)
		touchedDirs := make(map[string]struct{})
		for _, w := range plan.writes {
			gbs.mergeCacheObjectLocked(w.path, w.oid, git.ObjectTypeBlob, false)
			parent, base := splitGitPathParentBase(w.path)
			if base != "" {
				child := git.TreeEntry{Mode: "100644", Type: git.ObjectTypeBlob, OID: w.oid, Name: base}
				if gbs.mergeCacheChildLocked(parent, child, false) {
					touchedDirs[parent] = struct{}{}
				}
			}
		}
		gbs.sortCacheChildrenLocked(touchedDirs)
	} else if len(plan.writes) == 1 {
		w := plan.writes[0]
		gbs.mergeCacheObjectLocked(w.path, w.oid, git.ObjectTypeBlob, false)
	}
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

	_, partOIDs, _, err := gbs.hashParts(ctx, reader)
	if err != nil {
		return putPlan{}, err
	}
	if len(partOIDs) == 0 {
		return putPlan{}, fmt.Errorf("gitblobstore: chunked write for key %q produced no parts", key)
	}

	writes := make([]treeWrite, 0, len(partOIDs))
	for i, p := range partOIDs {
		partName := fmt.Sprintf("%0*d", gitblobstorePartNameWidth, i+1)
		writes = append(writes, treeWrite{path: key + "/" + partName, oid: p})
	}
	return putPlan{writes: writes, chunked: true}, nil
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

func (gbs *GitBlobstore) buildCommitForKeyWrite(ctx context.Context, parent git.OID, hasParent bool, key string, plan putPlan, msg string, extraWrites []pendingWrite) (git.OID, error) {
	_, indexFile, cleanup, err := git.NewTempIndex()
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

	// Apply extra pending writes first (used when flushing deferred writes with manifest).
	for _, pw := range extraWrites {
		if hasParent {
			if err := gbs.removeKeyConflictsFromIndex(ctx, parent, indexFile, pw.key, pw.plan.chunked); err != nil {
				return "", err
			}
		}
		for _, w := range pw.plan.writes {
			if err := gbs.api.UpdateIndexCacheInfo(ctx, indexFile, "100644", w.oid, w.path); err != nil {
				return "", err
			}
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

func (gbs *GitBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	key, err := normalizeGitTreePath(key)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: checkandput %s", key)

	// For the manifest key, flush all pending writes in a single commit+push.
	if key == gitblobstoreManifestKey {
		gbs.mu.Lock()
		pending := gbs.pendingWrites
		gbs.pendingWrites = nil
		gbs.mu.Unlock()
		ver, err := gbs.checkAndPutWithRemoteSync(ctx, expectedVersion, key, totalSize, reader, msg, pending)
		if err != nil && len(pending) > 0 {
			gbs.mu.Lock()
			gbs.pendingWrites = append(pending, gbs.pendingWrites...)
			gbs.mu.Unlock()
		}
		return ver, err
	}

	return gbs.checkAndPutWithRemoteSync(ctx, expectedVersion, key, totalSize, reader, msg, nil)
}

func (gbs *GitBlobstore) currentKeyVersion(ctx context.Context, commit git.OID, haveCommit bool, key string) (string, error) {
	if !haveCommit {
		// Ref missing => empty store => key missing.
		return "", nil
	}
	obj, ok := gbs.cacheGetObject(key)
	if !ok {
		return "", nil
	}
	return obj.oid.String(), nil
}

func (gbs *GitBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	// Keep key validation for consistent error behavior.
	var err error
	key, err = normalizeGitTreePath(key)
	if err != nil {
		return "", err
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

	// For non-manifest keys, skip remote sync and defer the write.
	if key != gitblobstoreManifestKey {
		return gbs.concatenateDeferred(ctx, key, sources)
	}

	// Manifest key: fall through to existing remote-synced path.
	if err := gbs.syncForRead(ctx); err != nil {
		return "", err
	}

	commit, ok, err := gbs.api.TryResolveRefCommit(ctx, gbs.localRef)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", NotFound{Key: key}
	}

	plan, err := gbs.planConcatenation(ctx, key, sources, commit)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: concatenate %s", key)
	return gbs.putWithRemoteSync(ctx, key, plan, msg)
}

// concatenateDeferred handles Concatenate for non-manifest keys without remote sync.
// Sources are read from the local cache/git objects (populated by prior Puts).
func (gbs *GitBlobstore) concatenateDeferred(ctx context.Context, key string, sources []string) (string, error) {
	// Fast-succeed if key already in cache.
	if obj, ok := gbs.cacheGetObject(key); ok {
		return obj.oid.String(), nil
	}

	plan, err := gbs.planConcatenation(ctx, key, sources, "")
	if err != nil {
		return "", err
	}

	return gbs.enqueuePendingWrite(key, plan), nil
}

// planConcatenation computes the total size of sources, opens a concatenated
// reader, and returns a putPlan for the result key.
func (gbs *GitBlobstore) planConcatenation(ctx context.Context, key string, sources []string, commit git.OID) (putPlan, error) {
	totalSz, err := gbs.totalSizeAtCommit(ctx, commit, sources)
	if err != nil {
		return putPlan{}, err
	}
	rc := &concatReadCloser{
		ctx:  ctx,
		keys: sources,
		open: func(ctx context.Context, k string) (io.ReadCloser, error) {
			return gbs.openReaderAtCommit(ctx, commit, k)
		},
	}
	defer rc.Close()
	return gbs.planPutWrites(ctx, key, totalSz, rc)
}

// enqueuePendingWrite appends a deferred write to pendingWrites, updates the
// cache optimistically, and returns a synthetic version string.
func (gbs *GitBlobstore) enqueuePendingWrite(key string, plan putPlan) string {
	gbs.mu.Lock()
	gbs.pendingWrites = append(gbs.pendingWrites, pendingWrite{key: key, plan: plan})
	gbs.mu.Unlock()
	gbs.cacheUpdateForPlan(key, plan)
	return plan.writes[0].oid.String()
}

func (gbs *GitBlobstore) openReaderAtCommit(ctx context.Context, commit git.OID, key string) (io.ReadCloser, error) {
	obj, ok := gbs.cacheGetObject(key)
	if !ok {
		return nil, NotFound{Key: key}
	}
	switch obj.typ {
	case git.ObjectTypeBlob:
		return gbs.api.BlobReader(ctx, obj.oid)
	case git.ObjectTypeTree:
		rc, _, err := gbs.openChunkedTreeRange(ctx, key, AllRange)
		if err != nil {
			return nil, err
		}
		return rc, nil
	default:
		return nil, fmt.Errorf("gitblobstore: unsupported object type %q for key %q", obj.typ, key)
	}
}

// sizeAtCommit returns the byte size of |key| as of |commit|.
// It supports both inline blobs and the chunked-tree representation used by GitBlobstore.
// If |key| is missing at |commit|, it returns NotFound{Key: key}.
func (gbs *GitBlobstore) sizeAtCommit(ctx context.Context, commit git.OID, key string) (uint64, error) {
	obj, ok := gbs.cacheGetObject(key)
	if !ok {
		return 0, NotFound{Key: key}
	}

	switch obj.typ {
	case git.ObjectTypeBlob:
		sz, err := gbs.api.BlobSize(ctx, obj.oid)
		if err != nil {
			return 0, err
		}
		if sz < 0 {
			return 0, fmt.Errorf("gitblobstore: invalid blob size %d for key %q", sz, key)
		}
		return uint64(sz), nil

	case git.ObjectTypeTree:
		entries, ok := gbs.cacheListChildren(key)
		if !ok {
			return 0, NotFound{Key: key}
		}
		_, total, err := gbs.validateAndSizeChunkedParts(ctx, entries)
		return total, err

	default:
		return 0, fmt.Errorf("gitblobstore: unsupported object type %q for key %q", obj.typ, key)
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
