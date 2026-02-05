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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type fakeGitAPI struct {
	tryResolveRefCommit func(ctx context.Context, ref string) (git.OID, bool, error)
	resolvePathBlob     func(ctx context.Context, commit git.OID, path string) (git.OID, error)
	blobSize            func(ctx context.Context, oid git.OID) (int64, error)
	blobReader          func(ctx context.Context, oid git.OID) (io.ReadCloser, error)
}

func (f fakeGitAPI) TryResolveRefCommit(ctx context.Context, ref string) (git.OID, bool, error) {
	return f.tryResolveRefCommit(ctx, ref)
}
func (f fakeGitAPI) ResolveRefCommit(ctx context.Context, ref string) (git.OID, error) {
	panic("unexpected call")
}
func (f fakeGitAPI) ResolvePathBlob(ctx context.Context, commit git.OID, path string) (git.OID, error) {
	return f.resolvePathBlob(ctx, commit, path)
}
func (f fakeGitAPI) CatFileType(ctx context.Context, oid git.OID) (string, error) {
	panic("unexpected call")
}
func (f fakeGitAPI) BlobSize(ctx context.Context, oid git.OID) (int64, error) {
	return f.blobSize(ctx, oid)
}
func (f fakeGitAPI) BlobReader(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
	return f.blobReader(ctx, oid)
}
func (f fakeGitAPI) HashObject(ctx context.Context, contents io.Reader) (git.OID, error) {
	panic("unexpected call")
}
func (f fakeGitAPI) ReadTree(ctx context.Context, commit git.OID, indexFile string) error {
	panic("unexpected call")
}
func (f fakeGitAPI) ReadTreeEmpty(ctx context.Context, indexFile string) error {
	panic("unexpected call")
}
func (f fakeGitAPI) UpdateIndexCacheInfo(ctx context.Context, indexFile string, mode string, oid git.OID, path string) error {
	panic("unexpected call")
}
func (f fakeGitAPI) WriteTree(ctx context.Context, indexFile string) (git.OID, error) {
	panic("unexpected call")
}
func (f fakeGitAPI) CommitTree(ctx context.Context, tree git.OID, parent *git.OID, message string, author *git.Identity) (git.OID, error) {
	panic("unexpected call")
}
func (f fakeGitAPI) UpdateRefCAS(ctx context.Context, ref string, newOID git.OID, oldOID git.OID, msg string) error {
	panic("unexpected call")
}
func (f fakeGitAPI) UpdateRef(ctx context.Context, ref string, newOID git.OID, msg string) error {
	panic("unexpected call")
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (t *trackingReadCloser) Close() error {
	t.closed = true
	return nil
}

func TestGitBlobstoreHelpers_resolveCommitForGet(t *testing.T) {
	ctx := context.Background()

	t.Run("ok", func(t *testing.T) {
		api := fakeGitAPI{
			tryResolveRefCommit: func(ctx context.Context, ref string) (git.OID, bool, error) {
				require.Equal(t, DoltDataRef, ref)
				return git.OID("0123456789abcdef0123456789abcdef01234567"), true, nil
			},
		}
		gbs := &GitBlobstore{ref: DoltDataRef, api: api}

		commit, ver, err := gbs.resolveCommitForGet(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, git.OID("0123456789abcdef0123456789abcdef01234567"), commit)
		require.Equal(t, "0123456789abcdef0123456789abcdef01234567", ver)
	})

	t.Run("missingRef_manifestIsNotFound", func(t *testing.T) {
		api := fakeGitAPI{
			tryResolveRefCommit: func(ctx context.Context, ref string) (git.OID, bool, error) {
				return git.OID(""), false, nil
			},
		}
		gbs := &GitBlobstore{ref: DoltDataRef, api: api}

		_, _, err := gbs.resolveCommitForGet(ctx, "manifest")
		var nf NotFound
		require.ErrorAs(t, err, &nf)
		require.Equal(t, "manifest", nf.Key)
	})

	t.Run("missingRef_nonManifestIsRefNotFound", func(t *testing.T) {
		api := fakeGitAPI{
			tryResolveRefCommit: func(ctx context.Context, ref string) (git.OID, bool, error) {
				return git.OID(""), false, nil
			},
		}
		gbs := &GitBlobstore{ref: DoltDataRef, api: api}

		_, _, err := gbs.resolveCommitForGet(ctx, "somekey")
		var rnf *git.RefNotFoundError
		require.ErrorAs(t, err, &rnf)
		require.Equal(t, DoltDataRef, rnf.Ref)
	})

	t.Run("propagatesError", func(t *testing.T) {
		sentinel := errors.New("boom")
		api := fakeGitAPI{
			tryResolveRefCommit: func(ctx context.Context, ref string) (git.OID, bool, error) {
				return git.OID(""), false, sentinel
			},
		}
		gbs := &GitBlobstore{ref: DoltDataRef, api: api}

		_, _, err := gbs.resolveCommitForGet(ctx, "k")
		require.ErrorIs(t, err, sentinel)
	})
}

func TestGitBlobstoreHelpers_resolveBlobForGet(t *testing.T) {
	ctx := context.Background()
	commit := git.OID("0123456789abcdef0123456789abcdef01234567")

	t.Run("ok", func(t *testing.T) {
		api := fakeGitAPI{
			resolvePathBlob: func(ctx context.Context, gotCommit git.OID, path string) (git.OID, error) {
				require.Equal(t, commit, gotCommit)
				require.Equal(t, "k", path)
				return git.OID("89abcdef0123456789abcdef0123456789abcdef"), nil
			},
		}
		gbs := &GitBlobstore{api: api}

		oid, ver, err := gbs.resolveBlobForGet(ctx, commit, "k")
		require.NoError(t, err)
		require.Equal(t, "0123456789abcdef0123456789abcdef01234567", ver)
		require.Equal(t, git.OID("89abcdef0123456789abcdef0123456789abcdef"), oid)
	})

	t.Run("pathNotFoundMapsToNotFound", func(t *testing.T) {
		api := fakeGitAPI{
			resolvePathBlob: func(ctx context.Context, gotCommit git.OID, path string) (git.OID, error) {
				return git.OID(""), &git.PathNotFoundError{Commit: gotCommit.String(), Path: path}
			},
		}
		gbs := &GitBlobstore{api: api}

		_, ver, err := gbs.resolveBlobForGet(ctx, commit, "k")
		require.Equal(t, commit.String(), ver)
		var nf NotFound
		require.ErrorAs(t, err, &nf)
		require.Equal(t, "k", nf.Key)
	})
}

func TestGitBlobstoreHelpers_resolveBlobSizeForGet(t *testing.T) {
	ctx := context.Background()
	commit := git.OID("0123456789abcdef0123456789abcdef01234567")
	oid := git.OID("89abcdef0123456789abcdef0123456789abcdef")

	t.Run("ok", func(t *testing.T) {
		api := fakeGitAPI{
			blobSize: func(ctx context.Context, gotOID git.OID) (int64, error) {
				require.Equal(t, oid, gotOID)
				return 123, nil
			},
		}
		gbs := &GitBlobstore{api: api}

		sz, ver, err := gbs.resolveBlobSizeForGet(ctx, commit, oid)
		require.NoError(t, err)
		require.Equal(t, commit.String(), ver)
		require.Equal(t, int64(123), sz)
	})
}

func TestGitBlobstoreHelpers_reopenInlineBlobReaderClosesOriginal(t *testing.T) {
	ctx := context.Background()
	blobOID := git.OID("0123456789abcdef0123456789abcdef01234567")

	orig := &trackingReadCloser{Reader: bytes.NewReader([]byte("x"))}
	api := fakeGitAPI{
		blobReader: func(ctx context.Context, gotOID git.OID) (io.ReadCloser, error) {
			require.Equal(t, blobOID, gotOID)
			return io.NopCloser(bytes.NewReader([]byte("y"))), nil
		},
	}
	gbs := &GitBlobstore{api: api}

	rc, err := gbs.reopenInlineBlobReader(ctx, orig, blobOID)
	require.NoError(t, err)
	require.True(t, orig.closed)
	require.NotNil(t, rc)
	_ = rc.Close()
}

func TestReadAtMost(t *testing.T) {
	out, err := readAtMost(bytes.NewReader([]byte("hello")), 3)
	require.NoError(t, err)
	require.Equal(t, []byte("hel"), out)

	out, err = readAtMost(bytes.NewReader([]byte("hi")), 3)
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), out)
}

func TestReadFullBlobBounded(t *testing.T) {
	// Reads through to blobSize when within max.
	// Note: |already| is expected to be a prefix read from |r|, so |r| must represent the
	// remaining stream after the prefix has been consumed.
	r := bytes.NewReader([]byte("cdef"))
	got, err := readFullBlobBounded(r, []byte("ab"), 6, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("abcdef"), got)

	// Errors if blobSize exceeds max and we hit the cap.
	r = bytes.NewReader(bytes.Repeat([]byte("x"), 100))
	_, err = readFullBlobBounded(r, bytes.Repeat([]byte("x"), 10), 100000, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "descriptor too large")
}
