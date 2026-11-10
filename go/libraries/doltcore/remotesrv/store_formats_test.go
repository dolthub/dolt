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

func toyKey(h hash.Hash) []byte {
	return []byte{'k', h[0]}
}

// toyFormats requires capability |required|, refuses files that fail to add with |refused|, and gives each record its
// toyKey.
type toyFormats struct {
	required remotesapi.ClientCapability
	refused  error
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
	return &toyLocator{}
}

// toyLocator gives each located record its toyKey under a new key id.
type toyLocator struct {
	plainLocator
	keys []*remotesapi.StreamChunkLocationsResponse_DataKey
	ids  uint32
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
	l.keys = append(l.keys, &remotesapi.StreamChunkLocationsResponse_DataKey{KeyId: l.ids, WrappedKey: toyKey(h)})
}

func (l *toyLocator) newKeys() []*remotesapi.StreamChunkLocationsResponse_DataKey {
	ret := l.keys
	l.keys = nil
	return ret
}

// testDBCache serves |cs|, recording the context of each request.
type testDBCache struct {
	cs   RemoteSrvStore
	mu   sync.Mutex
	last context.Context
}

func (c *testDBCache) Get(ctx context.Context, _, _ string) (RemoteSrvStore, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = ctx
	return c.cs, nil
}

// keyCheckingFormat opens a chunk only with its toyKey.
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

func TestStoreFormats(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "repo"), os.ModePerm))
	cs, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), filepath.Join(dir, "repo"), 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	t.Cleanup(func() { cs.Close() })
	hashes := make(hash.HashSet)
	var hashList [][]byte
	var flat []byte
	for i := 0; i < 16; i++ {
		c := chunks.NewChunk([]byte(fmt.Sprintf("chunk %d", i)))
		require.NoError(t, cs.Put(ctx, c, func(chunks.Chunk) chunks.InsertAddrsCb {
			return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
		}))
		h := c.Hash()
		hashes.Insert(h)
		hashList = append(hashList, h[:])
		flat = append(flat, h[:]...)
	}
	_, err = cs.Commit(ctx, hash.New(hashList[0]), hash.Hash{})
	require.NoError(t, err)

	fs, err := filesys.LocalFS.WithWorkingDir(dir)
	require.NoError(t, err)
	cache := &testDBCache{cs: cs}
	srv, err := NewServer(ServerArgs{HttpListenAddr: "127.0.0.1:0", GrpcListenAddr: "127.0.0.1:0", FS: fs, DBCache: cache})
	require.NoError(t, err)
	listeners, err := srv.Listeners()
	require.NoError(t, err)
	go srv.Serve(listeners)
	t.Cleanup(srv.GracefulStop)
	addr := listeners.http.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := remotesapi.NewChunkStoreServiceClient(conn)
	toy := &toyFormats{}
	prev := storeFormats
	t.Cleanup(func() { storeFormats = prev })
	storeFormats = toy

	t.Run("LocatedKeys", func(t *testing.T) {
		dcs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "repo", addr, false, client, keyCheckingFormat{})
		require.NoError(t, err)
		defer dcs.Close()
		var mu sync.Mutex
		got := make(hash.HashSet)
		require.NoError(t, dcs.GetManyCompressed(ctx, hashes.Copy(), func(_ context.Context, c nbs.ToChunker) {
			mu.Lock()
			defer mu.Unlock()
			got.Insert(c.Hash())
		}))
		assert.Equal(t, hashes, got)

		req := &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: hashList}
		unary, err := client.GetDownloadLocations(ctx, req)
		require.NoError(t, err)
		dl, err := client.StreamDownloadLocations(ctx)
		require.NoError(t, err)
		require.NoError(t, dl.Send(req))
		streamed, err := dl.Recv()
		require.NoError(t, err)
		for _, loc := range append(unary.Locs, streamed.Locs...) {
			hgr := loc.GetHttpGetRange()
			for _, rc := range hgr.Ranges {
				require.NotZero(t, rc.KeyIndex)
				assert.Equal(t, toyKey(hash.New(rc.Hash)), hgr.Keys[rc.KeyIndex-1])
			}
		}
	})

	t.Run("ClientCapability", func(t *testing.T) {
		need := remotesapi.ClientCapability_CLIENT_CAPABILITY_ENCRYPTION
		toy.required = need
		defer func() { toy.required = 0 }()
		for name, rpc := range map[string]func([]remotesapi.ClientCapability) error{
			"GetDownloadLocations": func(caps []remotesapi.ClientCapability) error {
				_, err := client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: hashList, ClientCapabilities: caps})
				return err
			},
			"ListTableFiles": func(caps []remotesapi.ClientCapability) error {
				_, err := client.ListTableFiles(ctx, &remotesapi.ListTableFilesRequest{RepoPath: "repo", ClientCapabilities: caps})
				return err
			},
			"StreamDownloadLocations": func(caps []remotesapi.ClientCapability) error {
				s, err := client.StreamDownloadLocations(ctx)
				require.NoError(t, err)
				require.NoError(t, s.Send(&remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: hashList, ClientCapabilities: caps}))
				_, err = s.Recv()
				return err
			},
			"StreamChunkLocations": func(caps []remotesapi.ClientCapability) error {
				s, err := client.StreamChunkLocations(ctx)
				require.NoError(t, err)
				require.NoError(t, s.Send(&remotesapi.StreamChunkLocationsRequest{RepoPath: "repo", ChunkHashes: flat, ClientCapabilities: caps}))
				_, err = s.Recv()
				return err
			},
		} {
			assert.Equal(t, codes.FailedPrecondition, status.Code(rpc(nil)), name)
			assert.NoError(t, rpc([]remotesapi.ClientCapability{need}), name)
		}
	})

	t.Run("Refusal", func(t *testing.T) {
		toy.refused = nbs.ErrTableFileNotFound
		missing := hash.Of([]byte("missing table file"))
		info := []*remotesapi.ChunkTableInfo{{Hash: missing[:], ChunkCount: 1}}
		format := &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}
		_, err := client.AddTableFiles(ctx, &remotesapi.AddTableFilesRequest{RepoPath: "repo", ChunkTableInfo: info, ClientRepoFormat: format})
		assert.Equal(t, "refused", status.Convert(err).Message())
		_, err = client.Commit(ctx, &remotesapi.CommitRequest{RepoPath: "repo", ChunkTableInfo: info, Current: missing[:], Last: missing[:], ClientRepoFormat: format})
		assert.Equal(t, "refused", status.Convert(err).Message())
	})

	t.Run("RequestedRepoFormat", func(t *testing.T) {
		scope := bytes.Repeat([]byte{7}, encryptionScopeLen)
		f := &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion, EncryptionScope: scope}
		_, err := client.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{RepoPath: "repo", ClientRepoFormat: f})
		require.NoError(t, err)
		cache.mu.Lock()
		defer cache.mu.Unlock()
		got, ok := RequestedRepoFormat(cache.last)
		require.True(t, ok)
		assert.Equal(t, scope, got.EncryptionScope)
	})
}

func TestValidateEncryptionScope(t *testing.T) {
	for _, tc := range []struct {
		scope []byte
		ok    bool
	}{
		{nil, true},
		{bytes.Repeat([]byte{1}, encryptionScopeLen), true},
		{[]byte{1}, false},
		{make([]byte, encryptionScopeLen), false},
		{bytes.Repeat([]byte{1}, encryptionScopeLen+1), false},
	} {
		f := &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: "5", EncryptionScope: tc.scope}
		err := ValidateGetRepoMetadataRequest(&remotesapi.GetRepoMetadataRequest{RepoPath: "org/repo", ClientRepoFormat: f})
		assert.Equal(t, tc.ok, err == nil, "%x", tc.scope)
	}
}
