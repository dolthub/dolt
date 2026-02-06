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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type fakeGitAPI struct {
	tryResolveRefCommit func(ctx context.Context, ref string) (git.OID, bool, error)
	resolvePathBlob     func(ctx context.Context, commit git.OID, path string) (git.OID, error)
	resolvePathObject   func(ctx context.Context, commit git.OID, path string) (git.OID, string, error)
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
func (f fakeGitAPI) ResolvePathObject(ctx context.Context, commit git.OID, path string) (git.OID, string, error) {
	return f.resolvePathObject(ctx, commit, path)
}
func (f fakeGitAPI) ListTree(ctx context.Context, commit git.OID, treePath string) ([]git.TreeEntry, error) {
	panic("unexpected call")
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
func (f fakeGitAPI) RemoveIndexPaths(ctx context.Context, indexFile string, paths []string) error {
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

func TestGitBlobstoreHelpers_resolveObjectForGet(t *testing.T) {
	ctx := context.Background()
	commit := git.OID("0123456789abcdef0123456789abcdef01234567")

	t.Run("ok", func(t *testing.T) {
		api := fakeGitAPI{
			resolvePathObject: func(ctx context.Context, gotCommit git.OID, path string) (git.OID, string, error) {
				require.Equal(t, commit, gotCommit)
				require.Equal(t, "k", path)
				return git.OID("89abcdef0123456789abcdef0123456789abcdef"), "blob", nil
			},
		}
		gbs := &GitBlobstore{api: api}

		oid, typ, ver, err := gbs.resolveObjectForGet(ctx, commit, "k")
		require.NoError(t, err)
		require.Equal(t, "0123456789abcdef0123456789abcdef01234567", ver)
		require.Equal(t, "blob", typ)
		require.Equal(t, git.OID("89abcdef0123456789abcdef0123456789abcdef"), oid)
	})

	t.Run("pathNotFoundMapsToNotFound", func(t *testing.T) {
		api := fakeGitAPI{
			resolvePathObject: func(ctx context.Context, gotCommit git.OID, path string) (git.OID, string, error) {
				return git.OID(""), "", &git.PathNotFoundError{Commit: gotCommit.String(), Path: path}
			},
		}
		gbs := &GitBlobstore{api: api}

		_, _, ver, err := gbs.resolveObjectForGet(ctx, commit, "k")
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

func TestGitBlobstoreHelpers_validateAndSizeChunkedParts(t *testing.T) {
	ctx := context.Background()

	api := fakeGitAPI{
		blobSize: func(ctx context.Context, oid git.OID) (int64, error) {
			switch oid {
			case "0123456789abcdef0123456789abcdef01234567":
				return 3, nil
			case "89abcdef0123456789abcdef0123456789abcdef":
				return 5, nil
			default:
				return 0, errors.New("unexpected oid")
			}
		},
	}
	gbs := &GitBlobstore{api: api}

	parts, total, err := gbs.validateAndSizeChunkedParts(ctx, []git.TreeEntry{
		{Name: "0001", Type: "blob", OID: "0123456789abcdef0123456789abcdef01234567"},
		{Name: "0002", Type: "blob", OID: "89abcdef0123456789abcdef0123456789abcdef"},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8), total)
	require.Len(t, parts, 2)
	require.Equal(t, "0123456789abcdef0123456789abcdef01234567", parts[0].OIDHex)
	require.Equal(t, uint64(3), parts[0].Size)

	_, _, err = gbs.validateAndSizeChunkedParts(ctx, []git.TreeEntry{{Name: "1", Type: "blob", OID: "0123456789abcdef0123456789abcdef01234567"}})
	require.Error(t, err)
}
