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

package nbs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

type gatingBlobstore struct {
	inner blobstore.Blobstore
	gate  *putGate
}

func (g gatingBlobstore) Path() string {
	return g.inner.Path()
}

func (g gatingBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	return g.inner.Exists(ctx, key)
}

func (g gatingBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (rc io.ReadCloser, size uint64, ver string, err error) {
	return g.inner.Get(ctx, key, br)
}

func (g gatingBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	g.gate.maybeGatePut(ctx, key)
	return g.inner.Put(ctx, key, totalSize, reader)
}

func (g gatingBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	return g.inner.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
}

func (g gatingBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return g.inner.Concatenate(ctx, key, sources)
}

type putGate struct {
	mu sync.Mutex

	recordsStarted bool
	tailStarted    bool

	releaseCh chan struct{}
	released  bool

	forcedRelease bool
}

func newPutGate() *putGate {
	return &putGate{
		releaseCh: make(chan struct{}),
	}
}

func (g *putGate) maybeGatePut(ctx context.Context, key string) {
	isRecords := strings.HasSuffix(key, ".records")
	isTail := strings.HasSuffix(key, ".tail")
	if !isRecords && !isTail {
		return
	}

	g.mu.Lock()
	if isRecords {
		g.recordsStarted = true
	}
	if isTail {
		g.tailStarted = true
	}

	shouldRelease := (g.recordsStarted && g.tailStarted) && !g.released
	releaseCh := g.releaseCh
	g.mu.Unlock()

	if shouldRelease {
		g.mu.Lock()
		if !g.released && g.recordsStarted && g.tailStarted {
			g.released = true
			close(g.releaseCh)
		}
		g.mu.Unlock()
	}

	select {
	case <-releaseCh:
		return
	case <-time.After(2 * time.Second):
		// If NBS stops writing .records and .tail concurrently, we don't want to hang the test.
		// Force release so the write can proceed, and let the test assert this didn't happen.
		g.mu.Lock()
		if !g.released {
			g.forcedRelease = true
			g.released = true
			close(g.releaseCh)
		}
		g.mu.Unlock()
		return
	case <-ctx.Done():
		return
	}
}

func (g *putGate) assertSawConcurrentPuts(t *testing.T) {
	t.Helper()

	g.mu.Lock()
	defer g.mu.Unlock()

	require.True(t, g.recordsStarted, "did not observe .records put")
	require.True(t, g.tailStarted, "did not observe .tail put")
	require.False(t, g.forcedRelease, "timed out waiting for concurrent .records + .tail puts")
}

type observingBlobstore struct {
	inner blobstore.Blobstore

	mu         sync.Mutex
	maxSources int
}

type manifestInterloperBlobstore struct {
	inner blobstore.Blobstore

	mu                    sync.Mutex
	didInterlope          bool
	checkAndPutCASFailure int
}

type manifestValidatingBlobstore struct {
	inner blobstore.Blobstore
	t     *testing.T

	mu               sync.Mutex
	sawManifestWrite bool
}

type manifestExpectedVersionBlobstore struct {
	inner blobstore.Blobstore
	t     *testing.T

	mu             sync.Mutex
	sawManifestCAS bool
}

func assertManifestBytesHaveNBFVersion(t *testing.T, b []byte) {
	t.Helper()

	s := string(b)
	require.True(t, strings.HasPrefix(s, StorageVersion+":"), "manifest did not start with expected storage version %q: %q", StorageVersion, s)
	parts := strings.Split(s, ":")
	require.GreaterOrEqual(t, len(parts), 2, "manifest missing nbf version field: %q", s)
	require.NotEmpty(t, parts[1], "manifest wrote empty nbf version: %q", s)
}

func (m *manifestValidatingBlobstore) Path() string {
	return m.inner.Path()
}

func (m *manifestValidatingBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	return m.inner.Exists(ctx, key)
}

func (m *manifestValidatingBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return m.inner.Get(ctx, key, br)
}

func (m *manifestValidatingBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return m.inner.Put(ctx, key, totalSize, reader)
}

func (m *manifestValidatingBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	if key == manifestFile {
		b, err := io.ReadAll(reader)
		require.NoError(m.t, err)
		if totalSize != int64(len(b)) {
			require.Failf(m.t, "manifest size mismatch", "expected totalSize=%d but read %d", totalSize, len(b))
		}
		assertManifestBytesHaveNBFVersion(m.t, b)

		m.mu.Lock()
		m.sawManifestWrite = true
		m.mu.Unlock()

		reader = bytes.NewReader(b)
		totalSize = int64(len(b))
	}
	return m.inner.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
}

func (m *manifestValidatingBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return m.inner.Concatenate(ctx, key, sources)
}

func (m *manifestValidatingBlobstore) assertSawManifestWrite(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.True(t, m.sawManifestWrite, "expected at least one manifest write")
}

func (m *manifestExpectedVersionBlobstore) Path() string {
	return m.inner.Path()
}

func (m *manifestExpectedVersionBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	return m.inner.Exists(ctx, key)
}

func (m *manifestExpectedVersionBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return m.inner.Get(ctx, key, br)
}

func (m *manifestExpectedVersionBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return m.inner.Put(ctx, key, totalSize, reader)
}

func (m *manifestExpectedVersionBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	if key == manifestFile {
		require.NotEmpty(m.t, expectedVersion, "manifest CheckAndPut expectedVersion must not be empty")

		b, err := io.ReadAll(reader)
		require.NoError(m.t, err)
		if totalSize != int64(len(b)) {
			require.Failf(m.t, "manifest size mismatch", "expected totalSize=%d but read %d", totalSize, len(b))
		}
		assertManifestBytesHaveNBFVersion(m.t, b)

		m.mu.Lock()
		m.sawManifestCAS = true
		m.mu.Unlock()

		reader = bytes.NewReader(b)
		totalSize = int64(len(b))
	}
	return m.inner.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
}

func (m *manifestExpectedVersionBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return m.inner.Concatenate(ctx, key, sources)
}

func (m *manifestExpectedVersionBlobstore) assertSawManifestCAS(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.True(t, m.sawManifestCAS, "expected at least one manifest CheckAndPut call")
}

func (m *manifestInterloperBlobstore) Path() string {
	return m.inner.Path()
}

func (m *manifestInterloperBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	return m.inner.Exists(ctx, key)
}

func (m *manifestInterloperBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return m.inner.Get(ctx, key, br)
}

func (m *manifestInterloperBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return m.inner.Put(ctx, key, totalSize, reader)
}

func (m *manifestInterloperBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	// Force exactly one CAS miss on the manifest by advancing the store version
	// between the read of the manifest version and this CheckAndPut call.
	var doInterlope bool
	m.mu.Lock()
	doInterlope = !m.didInterlope && key == manifestFile
	if doInterlope {
		m.didInterlope = true
	}
	m.mu.Unlock()

	if doInterlope {
		_, _ = m.inner.Put(ctx, "manifest-interloper", int64(len("x")), strings.NewReader("x"))
	}

	ver, err := m.inner.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
	if blobstore.IsCheckAndPutError(err) {
		m.mu.Lock()
		m.checkAndPutCASFailure++
		m.mu.Unlock()
	}
	return ver, err
}

func (m *manifestInterloperBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	return m.inner.Concatenate(ctx, key, sources)
}

func (m *manifestInterloperBlobstore) assertForcedManifestCASMiss(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.True(t, m.didInterlope, "expected interloper to run")
	require.GreaterOrEqual(t, m.checkAndPutCASFailure, 1, "expected at least one manifest CheckAndPut CAS failure")
}

func (o *observingBlobstore) Path() string {
	return o.inner.Path()
}

func (o *observingBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	return o.inner.Exists(ctx, key)
}

func (o *observingBlobstore) Get(ctx context.Context, key string, br blobstore.BlobRange) (io.ReadCloser, uint64, string, error) {
	return o.inner.Get(ctx, key, br)
}

func (o *observingBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	return o.inner.Put(ctx, key, totalSize, reader)
}

func (o *observingBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	return o.inner.CheckAndPut(ctx, expectedVersion, key, totalSize, reader)
}

func (o *observingBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	o.mu.Lock()
	if len(sources) > o.maxSources {
		o.maxSources = len(sources)
	}
	o.mu.Unlock()
	return o.inner.Concatenate(ctx, key, sources)
}

func (o *observingBlobstore) assertSawConjoinConcat(t *testing.T, minSources int) {
	t.Helper()

	o.mu.Lock()
	defer o.mu.Unlock()
	require.GreaterOrEqual(t, o.maxSources, minSources, "expected a Concatenate call with >= %d sources (saw %d)", minSources, o.maxSources)
}

func TestGitBlobstoreWriteSmoke_RoundTripRootValue(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8
	cs, err := NewBSStore(ctx, constants.FormatDefaultString, bs, memTableSize, qp)
	require.NoError(t, err)

	vs := types.NewValueStore(cs)

	// Write a small value, commit it as the new root.
	ref, err := vs.WriteValue(ctx, types.String("hello gitblobstore"))
	require.NoError(t, err)

	last, err := vs.Root(ctx)
	require.NoError(t, err)

	ok, err := vs.Commit(ctx, ref.TargetHash(), last)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, vs.Close())

	// Reopen and verify the committed root and value are readable.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
	require.NoError(t, err)
	vs2 := types.NewValueStore(cs2)
	defer func() { _ = vs2.Close() }()

	gotRoot, err := vs2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, ref.TargetHash(), gotRoot)

	gotVal, err := vs2.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String("hello gitblobstore"), gotVal)
}

func TestGitBlobstoreWriteSmoke_FirstPersistConcurrentPuts(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	inner, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	gate := newPutGate()
	bs := gatingBlobstore{inner: inner, gate: gate}

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8
	cs, err := NewBSStore(ctx, constants.FormatDefaultString, bs, memTableSize, qp)
	require.NoError(t, err)

	vs := types.NewValueStore(cs)

	// Ensure we flush to a table file on first persist.
	payload := "hello gitblobstore (concurrent first persist)"
	ref, err := vs.WriteValue(ctx, types.String(payload))
	require.NoError(t, err)

	last, err := vs.Root(ctx)
	require.NoError(t, err)

	ok, err := vs.Commit(ctx, ref.TargetHash(), last)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, vs.Close())

	gate.assertSawConcurrentPuts(t)

	// Reopen and verify: this catches the historical lost-update case where one of the
	// initial table-file puts could be overwritten when the backing ref doesn't exist yet.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
	require.NoError(t, err)
	vs2 := types.NewValueStore(cs2)
	defer func() { _ = vs2.Close() }()

	gotRoot, err := vs2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, ref.TargetHash(), gotRoot)

	gotVal, err := vs2.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String(payload), gotVal)
}

func TestGitBlobstoreWriteSmoke_ConjoinUsesConcatenate(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8

	// 1) Write a few commits to create multiple table files.
	bs, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs, err := NewBSStore(ctx, constants.FormatDefaultString, bs, memTableSize, qp)
	require.NoError(t, err)
	vs := types.NewValueStore(cs)

	var wantRoot types.Ref
	var wantVal string
	for i := 0; i < 3; i++ {
		wantVal = fmt.Sprintf("hello gitblobstore conjoin %d", i)
		ref, err := vs.WriteValue(ctx, types.String(wantVal))
		require.NoError(t, err)
		wantRoot = ref

		last, err := vs.Root(ctx)
		require.NoError(t, err)
		ok, err := vs.Commit(ctx, ref.TargetHash(), last)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, vs.Close())

	// 2) Reopen with an observing blobstore and run a conjoin. This should call Concatenate
	// with >= 3 sources when conjoining the table-record subobjects.
	inner2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	obs := &observingBlobstore{inner: inner2}
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, obs, memTableSize, qp)
	require.NoError(t, err)
	_, err = cs2.ConjoinTableFiles(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, cs2.Close())
	obs.assertSawConjoinConcat(t, 3)

	// 3) Reopen and verify data is still readable.
	bs3, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs3, err := NewBSStore(ctx, constants.FormatDefaultString, bs3, memTableSize, qp)
	require.NoError(t, err)
	vs3 := types.NewValueStore(cs3)
	defer func() { _ = vs3.Close() }()

	gotRoot, err := vs3.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, wantRoot.TargetHash(), gotRoot)

	gotVal, err := vs3.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String(wantVal), gotVal)
}

func TestGitBlobstoreWriteSmoke_ManifestCASContentionRetries(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8

	// Seed the store with a normal commit so the manifest/root are valid, then introduce
	// a CAS miss on a subsequent manifest update.
	{
		bs0, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
		require.NoError(t, err)
		cs0, err := NewBSStore(ctx, constants.FormatDefaultString, bs0, memTableSize, qp)
		require.NoError(t, err)
		vs0 := types.NewValueStore(cs0)

		seed := "hello gitblobstore seed commit"
		seedRef, err := vs0.WriteValue(ctx, types.String(seed))
		require.NoError(t, err)
		seedLast, err := vs0.Root(ctx)
		require.NoError(t, err)
		ok, err := vs0.Commit(ctx, seedRef.TargetHash(), seedLast)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, vs0.Close())
	}

	inner, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	interloper := &manifestInterloperBlobstore{inner: inner}

	cs, err := NewBSStore(ctx, constants.FormatDefaultString, interloper, memTableSize, qp)
	require.NoError(t, err)
	vs := types.NewValueStore(cs)

	want := "hello gitblobstore manifest cas contention"
	ref, err := vs.WriteValue(ctx, types.String(want))
	require.NoError(t, err)

	last, err := vs.Root(ctx)
	require.NoError(t, err)

	ok, err := vs.Commit(ctx, ref.TargetHash(), last)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, vs.Close())

	interloper.assertForcedManifestCASMiss(t)

	// Reopen and verify content is readable.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
	require.NoError(t, err)
	vs2 := types.NewValueStore(cs2)
	defer func() { _ = vs2.Close() }()

	gotRoot, err := vs2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, ref.TargetHash(), gotRoot)

	gotVal, err := vs2.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String(want), gotVal)
}

func TestGitBlobstoreWriteSmoke_BootstrapManifestHasNBFVersion_ThenManifestCASContentionRetries(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8

	// 1) Bootstrap a brand-new store and assert the manifest write always includes a non-empty nbf version.
	{
		inner, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
		require.NoError(t, err)
		validating := &manifestValidatingBlobstore{inner: inner, t: t}

		cs, err := NewBSStore(ctx, constants.FormatDefaultString, validating, memTableSize, qp)
		require.NoError(t, err)
		vs := types.NewValueStore(cs)

		seed := "hello gitblobstore bootstrap manifest validation"
		ref, err := vs.WriteValue(ctx, types.String(seed))
		require.NoError(t, err)
		last, err := vs.Root(ctx)
		require.NoError(t, err)
		ok, err := vs.Commit(ctx, ref.TargetHash(), last)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, vs.Close())

		validating.assertSawManifestWrite(t)
	}

	// 2) Now introduce a manifest CAS miss and assert we still succeed (retry path).
	{
		inner, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
		require.NoError(t, err)
		validating := &manifestValidatingBlobstore{inner: inner, t: t}
		interloper := &manifestInterloperBlobstore{inner: validating}

		cs, err := NewBSStore(ctx, constants.FormatDefaultString, interloper, memTableSize, qp)
		require.NoError(t, err)
		vs := types.NewValueStore(cs)

		want := "hello gitblobstore bootstrap then cas contention"
		ref, err := vs.WriteValue(ctx, types.String(want))
		require.NoError(t, err)
		last, err := vs.Root(ctx)
		require.NoError(t, err)
		ok, err := vs.Commit(ctx, ref.TargetHash(), last)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, vs.Close())

		interloper.assertForcedManifestCASMiss(t)
		validating.assertSawManifestWrite(t)

		// Reopen and verify content is readable.
		bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
		require.NoError(t, err)
		cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
		require.NoError(t, err)
		vs2 := types.NewValueStore(cs2)
		defer func() { _ = vs2.Close() }()

		gotRoot, err := vs2.Root(ctx)
		require.NoError(t, err)
		require.Equal(t, ref.TargetHash(), gotRoot)

		gotVal, err := vs2.ReadValue(ctx, gotRoot)
		require.NoError(t, err)
		require.Equal(t, types.String(want), gotVal)
	}
}

func TestGitBlobstoreWriteSmoke_ManifestMissingButTablesPresent_CreateUsesStoreVersion(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	// Seed refs/dolt/data with "table-like" blobs but no manifest, so the store has a non-empty
	// version even though the manifest key is missing.
	seedCommit, err := repo.SetRefToTree(ctx, blobstore.DoltDataRef, map[string][]byte{
		"seedtable.records": []byte("records"),
		"seedtable.tail":    []byte("tail"),
		"seedtable":         []byte("records+tail"),
	}, "seed table blobs without manifest")
	require.NoError(t, err)
	require.NotEmpty(t, seedCommit)

	inner, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	ok, err := inner.Exists(ctx, manifestFile)
	require.NoError(t, err)
	require.False(t, ok, "expected manifest to be missing after seeding")

	ok, err = inner.Exists(ctx, "seedtable")
	require.NoError(t, err)
	require.True(t, ok, "expected seeded table blob to exist")

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8

	verAsserting := &manifestExpectedVersionBlobstore{inner: inner, t: t}
	cs, err := NewBSStore(ctx, constants.FormatDefaultString, verAsserting, memTableSize, qp)
	require.NoError(t, err)
	vs := types.NewValueStore(cs)

	want := "hello gitblobstore manifest missing but tables present"
	ref, err := vs.WriteValue(ctx, types.String(want))
	require.NoError(t, err)
	last, err := vs.Root(ctx)
	require.NoError(t, err)

	ok2, err := vs.Commit(ctx, ref.TargetHash(), last)
	require.NoError(t, err)
	require.True(t, ok2)
	require.NoError(t, vs.Close())

	verAsserting.assertSawManifestCAS(t)

	// Reopen and verify readable.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
	require.NoError(t, err)
	vs2 := types.NewValueStore(cs2)
	defer func() { _ = vs2.Close() }()

	gotRoot, err := vs2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, ref.TargetHash(), gotRoot)

	gotVal, err := vs2.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String(want), gotVal)
}

func TestGitBlobstoreWriteSmoke_PostWriteTableAccessPatterns(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	memTableSize := uint64(0) // use default; avoids tiny-memtable edge cases during chunker writes

	// 1) Write a commit through NBS so we get a real table file persisted to the blobstore.
	{
		bs, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
		require.NoError(t, err)
		cs, err := NewBSStore(ctx, constants.FormatDefaultString, bs, memTableSize, qp)
		require.NoError(t, err)
		vs := types.NewValueStore(cs)

		// Build a value that should span enough chunks to produce a non-trivial table file.
		elems := make([]types.Value, 0, 2048)
		for i := 0; i < 2048; i++ {
			elems = append(elems, types.String(fmt.Sprintf("elem-%04d-%s", i, strings.Repeat("x", 64))))
		}
		lst, err := types.NewList(ctx, vs, elems...)
		require.NoError(t, err)

		ref, err := vs.WriteValue(ctx, lst)
		require.NoError(t, err)

		last, err := vs.Root(ctx)
		require.NoError(t, err)

		ok, err := vs.Commit(ctx, ref.TargetHash(), last)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, vs.Close())
	}

	// 2) Reopen and discover the table file key from the manifest.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	mrc, _, _, err := bs2.Get(ctx, manifestFile, blobstore.AllRange)
	require.NoError(t, err)
	mc, err := parseManifest(mrc)
	require.NoError(t, err)
	require.NoError(t, mrc.Close())
	require.NotEmpty(t, mc.specs, "expected at least one table spec in manifest")

	// Choose the largest table-like object referenced by the manifest. We want enough
	// bytes to exercise both tail reads and ranged reads.
	var (
		tableKey string
		tableSz  uint64
	)
	probe := func(key string) (uint64, bool) {
		ok, err := bs2.Exists(ctx, key)
		require.NoError(t, err)
		if !ok {
			return 0, false
		}
		rc, sz, _, err := bs2.Get(ctx, key, blobstore.NewBlobRange(0, 1))
		require.NoError(t, err)
		_ = rc.Close()
		return sz, true
	}
	for _, spec := range mc.specs {
		if sz, ok := probe(spec.name.String()); ok && sz > tableSz {
			tableKey, tableSz = spec.name.String(), sz
		}
		if sz, ok := probe(spec.name.String() + ArchiveFileSuffix); ok && sz > tableSz {
			tableKey, tableSz = spec.name.String()+ArchiveFileSuffix, sz
		}
	}
	require.NotEmpty(t, tableKey, "expected at least one readable table object")

	// Read the whole table once so we can validate range reads.
	trc, _, _, err := bs2.Get(ctx, tableKey, blobstore.AllRange)
	require.NoError(t, err)
	full, err := io.ReadAll(trc)
	require.NoError(t, err)
	require.NoError(t, trc.Close())
	require.Greater(t, len(full), 8*1024, "expected table file to be reasonably sized for range reads")

	// 3) Tail-read pattern used by table index/footer loads.
	const tailN = 1024
	rc, totalSz, _, err := bs2.Get(ctx, tableKey, blobstore.NewBlobRange(-tailN, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(len(full)), totalSz)
	tail := make([]byte, tailN)
	_, err = io.ReadFull(rc, tail)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, full[len(full)-tailN:], tail)

	// 4) ReadAt-style ranged reads used by table readers.
	stats := NewStats()
	readerAt := &bsTableReaderAt{bs: bs2, key: tableKey}
	out := make([]byte, 4096)
	const off = 1234
	n, err := readerAt.ReadAtWithStats(ctx, out, off, stats)
	require.NoError(t, err)
	require.Equal(t, len(out), n)
	require.Equal(t, full[off:off+int64(len(out))], out)
}
