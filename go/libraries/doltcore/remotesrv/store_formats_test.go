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

package remotesrv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// toyKey is the key a keyed toyFormats store gives the record of |h|.
func toyKey(h hash.Hash) []byte {
	return []byte{'k', h[0]}
}

type toyFormats struct {
	required remotesapi.ClientCapability
	refused  error
	keyed    bool
}

func (f toyFormats) refuseClient(_ string, caps []remotesapi.ClientCapability) error {
	if f.required != remotesapi.ClientCapability_CLIENT_CAPABILITY_UNSPECIFIED && !slices.Contains(caps, f.required) {
		return status.Error(codes.FailedPrecondition, "toy repository")
	}
	return nil
}

func (f toyFormats) refusal(err error) error {
	if errors.Is(err, f.refused) {
		return status.Error(codes.FailedPrecondition, "refused")
	}
	return nil
}

func (f toyFormats) locator() chunkLocator {
	if !f.keyed {
		return plainLocator{}
	}
	return &toyLocator{}
}

// toyLocator gives every located record its toyKey, numbering a new key for each.
type toyLocator struct {
	plainLocator
	ids     uint32
	pending []*remotesapi.StreamChunkLocationsResponse_DataKey
}

func (l *toyLocator) setRangeKeys(hgr *remotesapi.HttpGetRange) {
	for _, rc := range hgr.Ranges {
		hgr.Keys = append(hgr.Keys, toyKey(hash.New(rc.Hash)))
		rc.KeyIndex = uint32(len(hgr.Keys))
	}
}

func (l *toyLocator) setLocationKeys(loc *remotesapi.StreamChunkLocationsResponse_ChunkLocation, h hash.Hash) {
	l.ids++
	loc.KeyId = l.ids
	l.pending = append(l.pending, &remotesapi.StreamChunkLocationsResponse_DataKey{KeyId: l.ids, WrappedKey: toyKey(h)})
}

func (l *toyLocator) newKeys() []*remotesapi.StreamChunkLocationsResponse_DataKey {
	ret := l.pending
	l.pending = nil
	return ret
}

func withStoreFormats(t *testing.T, f serverFormats) {
	prev := storeFormats
	t.Cleanup(func() { storeFormats = prev })
	storeFormats = f
}

type testDBCache struct {
	cs   RemoteSrvStore
	mu   sync.Mutex
	ctxs []context.Context
}

func (c *testDBCache) Get(ctx context.Context, _, _ string) (RemoteSrvStore, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctxs = append(c.ctxs, ctx)
	return c.cs, nil
}

type testRemote struct {
	addr   string
	client remotesapi.ChunkStoreServiceClient
	hashes hash.HashSet
	cache  *testDBCache
}

// startTestRemote serves a store of |n| chunks in table files, from a server without |disabled|.
func startTestRemote(t *testing.T, n int, disabled ...remotesapi.Feature) testRemote {
	ctx := context.Background()
	dir := t.TempDir()
	repoDir := filepath.Join(dir, "repo")
	require.NoError(t, os.MkdirAll(repoDir, os.ModePerm))
	cs, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), repoDir, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	t.Cleanup(func() { cs.Close() })

	noRefs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}
	hashes := make(hash.HashSet)
	var last hash.Hash
	for i := 0; i < n; i++ {
		c := chunks.NewChunk([]byte(fmt.Sprintf("chunk %d of a keyed store", i)))
		require.NoError(t, cs.Put(ctx, c, noRefs))
		hashes.Insert(c.Hash())
		last = c.Hash()
	}
	ok, err := cs.Commit(ctx, last, hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)

	fs, err := filesys.LocalFS.WithWorkingDir(dir)
	require.NoError(t, err)
	cache := &testDBCache{cs: cs}
	srv, err := NewServer(ServerArgs{
		HttpListenAddr:   "127.0.0.1:0",
		GrpcListenAddr:   "127.0.0.1:0",
		FS:               fs,
		DBCache:          cache,
		DisabledFeatures: disabled,
	})
	require.NoError(t, err)
	listeners, err := srv.Listeners()
	require.NoError(t, err)
	go srv.Serve(listeners)
	t.Cleanup(srv.GracefulStop)

	addr := listeners.http.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return testRemote{addr: addr, client: remotesapi.NewChunkStoreServiceClient(conn), hashes: hashes, cache: cache}
}

func (r testRemote) hashList() [][]byte {
	var ret [][]byte
	for h := range r.hashes {
		ret = append(ret, h[:])
	}
	return ret
}

// keyCheckingFormat opens the chunks of a keyed toyFormats store, checking that each comes with its key.
type keyCheckingFormat struct {
	remotestorage.PlainFormat
}

func (f keyCheckingFormat) Opened(*remotesapi.GetRepoMetadataResponse) (remotestorage.RepoFormat, error) {
	return f, nil
}

func (f keyCheckingFormat) OpenChunk(ctx context.Context, h hash.Hash, key []byte, data []byte, dict *nbs.DecompBundle) (nbs.ToChunker, error) {
	if !bytes.Equal(key, toyKey(h)) {
		return nil, fmt.Errorf("chunk %s came with key %x", h, key)
	}
	return f.PlainFormat.OpenChunk(ctx, h, nil, data, dict)
}

func TestLocatedKeysReachTheClient(t *testing.T) {
	withStoreFormats(t, toyFormats{keyed: true})
	ctx := context.Background()
	for _, disabled := range [][]remotesapi.Feature{nil, {remotesapi.Feature_FEATURE_STREAM_CHUNK_LOCATIONS}} {
		r := startTestRemote(t, 16, disabled...)
		dcs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "repo", r.addr, false, r.client, keyCheckingFormat{})
		require.NoError(t, err)
		var mu sync.Mutex
		got := make(hash.HashSet)
		require.NoError(t, dcs.GetManyCompressed(ctx, r.hashes.Copy(), func(_ context.Context, c nbs.ToChunker) {
			mu.Lock()
			defer mu.Unlock()
			got.Insert(c.Hash())
		}))
		require.NoError(t, dcs.Close())
		assert.Equal(t, r.hashes, got, "disabled %v", disabled)
	}

	r := startTestRemote(t, 4)
	resp, err := r.client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList()})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Locs)
	for _, loc := range resp.Locs {
		hgr := loc.GetHttpGetRange()
		for _, rc := range hgr.Ranges {
			assert.Equal(t, toyKey(hash.New(rc.Hash)), hgr.Keys[rc.KeyIndex-1])
		}
	}
}

func TestClientCapabilityRefusal(t *testing.T) {
	need := remotesapi.ClientCapability_CLIENT_CAPABILITY_ENCRYPTION
	withStoreFormats(t, toyFormats{required: need})
	ctx := context.Background()
	r := startTestRemote(t, 4)
	var flat []byte
	for _, h := range r.hashList() {
		flat = append(flat, h...)
	}

	for _, caps := range [][]remotesapi.ClientCapability{nil, {need}} {
		check := func(err error) {
			if caps == nil {
				assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			} else {
				assert.NoError(t, err)
			}
		}
		_, err := r.client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList(), ClientCapabilities: caps})
		check(err)
		_, err = r.client.ListTableFiles(ctx, &remotesapi.ListTableFilesRequest{RepoPath: "repo", ClientCapabilities: caps})
		check(err)

		dl, err := r.client.StreamDownloadLocations(ctx)
		require.NoError(t, err)
		require.NoError(t, dl.Send(&remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList(), ClientCapabilities: caps}))
		_, err = dl.Recv()
		check(err)

		cl, err := r.client.StreamChunkLocations(ctx)
		require.NoError(t, err)
		require.NoError(t, cl.Send(&remotesapi.StreamChunkLocationsRequest{RepoPath: "repo", ChunkHashes: flat, ClientCapabilities: caps}))
		_, err = cl.Recv()
		check(err)
	}
}

func TestStoreFormatsRefusal(t *testing.T) {
	withStoreFormats(t, toyFormats{refused: nbs.ErrTableFileNotFound})
	ctx := context.Background()
	r := startTestRemote(t, 1)
	missing := hash.Of([]byte("missing table file"))
	info := []*remotesapi.ChunkTableInfo{{Hash: missing[:], ChunkCount: 1}}

	_, err := r.client.AddTableFiles(ctx, &remotesapi.AddTableFilesRequest{RepoPath: "repo", ChunkTableInfo: info, ClientRepoFormat: &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}})
	require.Error(t, err)
	assert.Equal(t, "refused", status.Convert(err).Message())

	_, err = r.client.Commit(ctx, &remotesapi.CommitRequest{RepoPath: "repo", ChunkTableInfo: info, Current: missing[:], Last: missing[:], ClientRepoFormat: &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}})
	require.Error(t, err)
	assert.Equal(t, "refused", status.Convert(err).Message())
}

func TestRequestedRepoFormat(t *testing.T) {
	_, ok := RequestedRepoFormat(context.Background())
	assert.False(t, ok)

	ctx := context.Background()
	r := startTestRemote(t, 1)
	scope := bytes.Repeat([]byte{7}, encryptionScopeLen)
	f := &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion, EncryptionScope: scope}
	_, err := r.client.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{RepoPath: "repo", ClientRepoFormat: f})
	require.NoError(t, err)
	_, err = r.client.Root(ctx, &remotesapi.RootRequest{RepoPath: "repo"})
	require.NoError(t, err)

	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	require.Len(t, r.cache.ctxs, 2)
	got, ok := RequestedRepoFormat(r.cache.ctxs[0])
	require.True(t, ok)
	assert.Equal(t, scope, got.EncryptionScope)
	_, ok = RequestedRepoFormat(r.cache.ctxs[1])
	assert.False(t, ok, "only GetRepoMetadata carries the format")
}

func TestValidateEncryptionScope(t *testing.T) {
	req := &remotesapi.GetRepoMetadataRequest{RepoPath: "org/repo", ClientRepoFormat: &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: "5"}}
	require.NoError(t, ValidateGetRepoMetadataRequest(req))
	req.ClientRepoFormat.EncryptionScope = bytes.Repeat([]byte{1}, encryptionScopeLen)
	require.NoError(t, ValidateGetRepoMetadataRequest(req))
	for _, b := range [][]byte{{1}, make([]byte, encryptionScopeLen), bytes.Repeat([]byte{1}, encryptionScopeLen+1)} {
		req.ClientRepoFormat.EncryptionScope = b
		assert.Error(t, ValidateGetRepoMetadataRequest(req))
	}
}
