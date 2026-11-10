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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type hookCalls []string

type recordingLocator struct{ calls *hookCalls }

func (l recordingLocator) locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error) {
	*l.calls = append(*l.calls, "locate")
	return cs.GetChunkLocationsWithPaths(ctx, hashes)
}

func (l recordingLocator) addRangeKeys(*remotesapi.HttpGetRange) error {
	*l.calls = append(*l.calls, "addRangeKeys")
	return nil
}

func (l recordingLocator) addLocationKeys(*remotesapi.StreamChunkLocationsResponse, []byte) error {
	*l.calls = append(*l.calls, "addLocationKeys")
	return nil
}

type storeCache struct{ cs RemoteSrvStore }

func (c storeCache) Get(context.Context, string, string) (RemoteSrvStore, error) {
	return c.cs, nil
}

type chunkLocationsStream struct {
	grpc.ServerStream
	reqs []*remotesapi.StreamChunkLocationsRequest
	sent []*remotesapi.StreamChunkLocationsResponse
}

func (s *chunkLocationsStream) Context() context.Context {
	return context.Background()
}

func (s *chunkLocationsStream) Recv() (*remotesapi.StreamChunkLocationsRequest, error) {
	if len(s.reqs) == 0 {
		return nil, io.EOF
	}
	req := s.reqs[0]
	s.reqs = s.reqs[1:]
	return req, nil
}

func (s *chunkLocationsStream) Send(resp *remotesapi.StreamChunkLocationsResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func TestStoreHooksAreReached(t *testing.T) {
	ctx := context.Background()
	var calls hookCalls
	refused := errors.New("refused")
	saved := []any{checkClient, checkClientRepoFormat, withClientRepoFormat, addRefusal, newChunkLocator}
	t.Cleanup(func() {
		checkClient = saved[0].(func(RemoteSrvStore, []remotesapi.ClientCapability) error)
		checkClientRepoFormat = saved[1].(func(*remotesapi.ClientRepoFormat) error)
		withClientRepoFormat = saved[2].(func(context.Context, *remotesapi.ClientRepoFormat) context.Context)
		addRefusal = saved[3].(func(error) error)
		newChunkLocator = saved[4].(func() chunkLocator)
	})
	checkClient = func(RemoteSrvStore, []remotesapi.ClientCapability) error {
		calls = append(calls, "checkClient")
		return nil
	}
	checkClientRepoFormat = func(*remotesapi.ClientRepoFormat) error {
		calls = append(calls, "checkClientRepoFormat")
		return nil
	}
	withClientRepoFormat = func(ctx context.Context, _ *remotesapi.ClientRepoFormat) context.Context {
		calls = append(calls, "withClientRepoFormat")
		return ctx
	}
	addRefusal = func(error) error {
		calls = append(calls, "addRefusal")
		return refused
	}
	newChunkLocator = func() chunkLocator { return recordingLocator{&calls} }

	dir := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.Mkdir(dir, 0755))
	cs, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	defer cs.Close()
	c := chunks.NewChunk([]byte("hooked"))
	require.NoError(t, cs.Put(ctx, c, func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}))
	_, err = cs.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	fs, err := filesys.LocalFilesysWithWorkingDir(filepath.Dir(dir))
	require.NoError(t, err)
	rs := NewHttpFSBackedChunkStore(logrus.NewEntry(logrus.New()), "localhost", storeCache{cs}, fs, "http", 0, identitySealer{}, nil)
	h := c.Hash()

	_, err = rs.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{RepoPath: "repo", ClientRepoFormat: &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}})
	require.NoError(t, err)
	require.Equal(t, hookCalls{"checkClientRepoFormat", "withClientRepoFormat"}, calls)

	calls = nil
	_, err = rs.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: [][]byte{h[:]}})
	require.NoError(t, err)
	require.Equal(t, hookCalls{"checkClient", "locate", "addRangeKeys"}, calls)

	calls = nil
	stream := &chunkLocationsStream{reqs: []*remotesapi.StreamChunkLocationsRequest{{RepoPath: "repo", ChunkHashes: h[:]}}}
	require.NoError(t, rs.StreamChunkLocations(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, hookCalls{"checkClient", "locate", "addLocationKeys"}, calls)

	calls = nil
	_, err = rs.ListTableFiles(ctx, &remotesapi.ListTableFilesRequest{RepoPath: "repo"})
	require.NoError(t, err)
	require.Equal(t, hookCalls{"checkClient"}, calls)

	calls = nil
	missing := hash.Of([]byte("missing"))
	_, err = rs.AddTableFiles(ctx, &remotesapi.AddTableFilesRequest{RepoPath: "repo", ChunkTableInfo: []*remotesapi.ChunkTableInfo{{Hash: missing[:], ChunkCount: 1}}})
	require.ErrorIs(t, err, refused)
	require.Equal(t, hookCalls{"addRefusal"}, calls)
}
