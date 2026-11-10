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

// toyKey is the stored key of the record of |h| in a toyKeys store; chunks share three keys.
func toyKey(h hash.Hash) []byte {
	return []byte{'k', h[0] % 3}
}

// withToyKeys makes every located record carry its toyKey.
func withToyKeys(t *testing.T) {
	withStoreFormats(t, toyFormats{keyed: true})
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

func (f toyFormats) locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, recordKeys, error) {
	locs, err := cs.GetChunkLocationsWithPaths(ctx, hashes)
	if !f.keyed {
		return locs, noKeys, err
	}
	return locs, func(h hash.Hash, _ uint64, _ uint32) ([]byte, []byte) { return toyKey(h), toyDictKey }, err
}

// toyDictKey is the key a toyFormats store gives every record's dictionary.
var toyDictKey = []byte("dict")

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

func (r testRemote) flatHashes() []byte {
	var ret []byte
	for _, h := range r.hashList() {
		ret = append(ret, h...)
	}
	return ret
}

// keyCheckingFormat opens the chunks of a toyKeys store, checking that each comes with its key.
type keyCheckingFormat struct {
	remotestorage.PlainFormat
	mu     *sync.Mutex
	opened map[hash.Hash][]byte
}

func (f keyCheckingFormat) Opened(*remotesapi.GetRepoMetadataResponse) (remotestorage.RepoFormat, error) {
	return f, nil
}

func (f keyCheckingFormat) OpenChunk(ctx context.Context, h hash.Hash, key []byte, data []byte, dict *nbs.DecompBundle) (nbs.ToChunker, error) {
	if !bytes.Equal(key, toyKey(h)) {
		return nil, fmt.Errorf("chunk %s came with key %x", h, key)
	}
	f.mu.Lock()
	f.opened[h] = key
	f.mu.Unlock()
	return f.PlainFormat.OpenChunk(ctx, h, nil, data, dict)
}

func TestLocatedKeysReachTheClient(t *testing.T) {
	for _, tc := range []struct {
		name     string
		disabled []remotesapi.Feature
	}{
		{"StreamChunkLocations", nil},
		{"StreamDownloadLocations", []remotesapi.Feature{remotesapi.Feature_FEATURE_STREAM_CHUNK_LOCATIONS}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withToyKeys(t)
			ctx := context.Background()
			r := startTestRemote(t, 64, tc.disabled...)

			format := keyCheckingFormat{mu: &sync.Mutex{}, opened: make(map[hash.Hash][]byte)}
			dcs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "repo", r.addr, false, r.client, format)
			require.NoError(t, err)
			defer dcs.Close()

			var mu sync.Mutex
			got := make(hash.HashSet)
			err = dcs.GetManyCompressed(ctx, r.hashes.Copy(), func(_ context.Context, c nbs.ToChunker) {
				mu.Lock()
				defer mu.Unlock()
				got.Insert(c.Hash())
			})
			require.NoError(t, err)
			assert.Equal(t, r.hashes, got)
			assert.Len(t, format.opened, len(r.hashes))
		})
	}
}

func TestLocatedKeysAreNumbered(t *testing.T) {
	withToyKeys(t)
	ctx := context.Background()
	r := startTestRemote(t, 32)

	checkRange := func(hgr *remotesapi.HttpGetRange) {
		distinct := map[string]bool{string(toyDictKey): true}
		for _, rc := range hgr.Ranges {
			h := hash.New(rc.Hash)
			require.NotZero(t, rc.KeyIndex)
			assert.Equal(t, toyKey(h), hgr.Keys[rc.KeyIndex-1])
			require.NotZero(t, rc.DictionaryKeyIndex)
			assert.Equal(t, toyDictKey, hgr.Keys[rc.DictionaryKeyIndex-1])
			distinct[string(toyKey(h))] = true
		}
		assert.Len(t, hgr.Keys, len(distinct), "each key appears once per range")
	}

	resp, err := r.client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList()})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Locs)
	for _, loc := range resp.Locs {
		checkRange(loc.GetHttpGetRange())
	}

	dl, err := r.client.StreamDownloadLocations(ctx)
	require.NoError(t, err)
	require.NoError(t, dl.Send(&remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList()}))
	dlResp, err := dl.Recv()
	require.NoError(t, err)
	for _, loc := range dlResp.Locs {
		checkRange(loc.GetHttpGetRange())
	}
	require.NoError(t, dl.CloseSend())

	stream, err := r.client.StreamChunkLocations(ctx)
	require.NoError(t, err)
	req := &remotesapi.StreamChunkLocationsRequest{RepoPath: "repo", ChunkHashes: r.flatHashes()}
	keysByID := make(map[uint32][]byte)
	for i := 0; i < 2; i++ {
		require.NoError(t, stream.Send(req))
		resp, err := stream.Recv()
		require.NoError(t, err)
		for _, k := range resp.Keys {
			_, dup := keysByID[k.KeyId]
			require.False(t, dup, "key %d introduced twice in one stream", k.KeyId)
			keysByID[k.KeyId] = k.WrappedKey
		}
		if i > 0 {
			assert.Empty(t, resp.Keys, "keys are introduced once per stream")
		}
		require.Len(t, resp.Locations, len(r.hashes))
		for _, cl := range resp.Locations {
			h := hash.New(req.ChunkHashes[cl.RequestIndex*hash.ByteLen : (cl.RequestIndex+1)*hash.ByteLen])
			assert.Equal(t, toyKey(h), keysByID[cl.KeyId])
			assert.Equal(t, toyDictKey, keysByID[cl.DictionaryKeyId])
		}
	}
	distinct := map[string]bool{string(toyDictKey): true}
	for h := range r.hashes {
		distinct[string(toyKey(h))] = true
	}
	assert.Len(t, keysByID, len(distinct))
	require.NoError(t, stream.CloseSend())
}

func TestAddRangeKeys(t *testing.T) {
	h := hash.Of([]byte("chunk"))
	hgr := &remotesapi.HttpGetRange{Ranges: []*remotesapi.RangeChunk{{Hash: h[:], Offset: 0}, {Hash: h[:], Offset: 10}, {Hash: h[:], Offset: 20}}}
	addRangeKeys(hgr, noKeys)
	assert.Empty(t, hgr.Keys)
	assert.Zero(t, hgr.Ranges[0].KeyIndex)

	addRangeKeys(hgr, func(_ hash.Hash, offset uint64, _ uint32) ([]byte, []byte) {
		switch offset {
		case 0:
			return []byte("a"), nil
		case 10:
			return []byte("b"), []byte("a")
		}
		return nil, nil
	})
	assert.Equal(t, [][]byte{[]byte("a"), []byte("b")}, hgr.Keys)
	assert.Equal(t, []uint32{1, 2, 0}, []uint32{hgr.Ranges[0].KeyIndex, hgr.Ranges[1].KeyIndex, hgr.Ranges[2].KeyIndex})
	assert.Equal(t, []uint32{0, 1, 0}, []uint32{hgr.Ranges[0].DictionaryKeyIndex, hgr.Ranges[1].DictionaryKeyIndex, hgr.Ranges[2].DictionaryKeyIndex})
}

func TestClientCapabilityRefusal(t *testing.T) {
	need := remotesapi.ClientCapability_CLIENT_CAPABILITY_ENCRYPTION
	withStoreFormats(t, toyFormats{required: need, keyed: true})
	ctx := context.Background()
	r := startTestRemote(t, 4)

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
		require.NoError(t, cl.Send(&remotesapi.StreamChunkLocationsRequest{RepoPath: "repo", ChunkHashes: r.flatHashes(), ClientCapabilities: caps}))
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

func TestKeyNumbering(t *testing.T) {
	var kn keyNumbering
	for _, tc := range []struct {
		key  string
		want uint32
	}{{"", 0}, {"a", 1}, {"b", 2}, {"a", 1}, {"c", 3}, {"b", 2}} {
		var key []byte
		if tc.key != "" {
			key = []byte(tc.key)
		}
		assert.Equal(t, tc.want, kn.ref(key))
	}
	dks := kn.dataKeys()
	require.Len(t, dks, 3)
	for i, dk := range dks {
		assert.Equal(t, uint32(i+1), dk.KeyId)
		assert.Equal(t, []byte{"abc"[i]}, dk.WrappedKey)
	}
	assert.Equal(t, uint32(4), kn.ref([]byte("d")))
	dks = kn.dataKeys()
	require.Len(t, dks, 1)
	assert.Equal(t, uint32(4), dks[0].KeyId)
	assert.Empty(t, kn.dataKeys())
}

// A plaintext store's locations carry no keys, so what goes over the wire is unchanged.
func TestPlaintextLocationsHaveNoKeys(t *testing.T) {
	withStoreFormats(t, plainFormats{})
	ctx := context.Background()
	r := startTestRemote(t, 16)

	resp, err := r.client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: r.hashList()})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Locs)
	for _, loc := range resp.Locs {
		hgr := loc.GetHttpGetRange()
		assert.Empty(t, hgr.Keys)
		for _, rc := range hgr.Ranges {
			assert.Zero(t, rc.KeyIndex)
			assert.Zero(t, rc.DictionaryKeyIndex)
		}
	}

	stream, err := r.client.StreamChunkLocations(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&remotesapi.StreamChunkLocationsRequest{RepoPath: "repo", ChunkHashes: r.flatHashes()}))
	sresp, err := stream.Recv()
	require.NoError(t, err)
	assert.Empty(t, sresp.Keys)
	for _, cl := range sresp.Locations {
		assert.Zero(t, cl.KeyId)
		assert.Zero(t, cl.DictionaryKeyId)
	}
	require.NoError(t, stream.CloseSend())
}
