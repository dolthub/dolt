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

type reachedHooks map[string]bool

type recordingLocator struct{ reached reachedHooks }

func (l recordingLocator) locate(ctx context.Context, cs RemoteSrvStore, hashes hash.HashSet) (map[string]map[hash.Hash]nbs.Range, error) {
	l.reached["locate"] = true
	return cs.GetChunkLocationsWithPaths(ctx, hashes)
}

func (l recordingLocator) addRangeKeys(*remotesapi.HttpGetRange) error {
	l.reached["addRangeKeys"] = true
	return nil
}

func (l recordingLocator) addLocationKeys(*remotesapi.StreamChunkLocationsResponse, []byte) error {
	l.reached["addLocationKeys"] = true
	return nil
}

type fakeServerStream[Req, Resp any] struct {
	grpc.ServerStream
	reqs []*Req
	sent []*Resp
}

func (s *fakeServerStream[Req, Resp]) Context() context.Context {
	return context.Background()
}

func (s *fakeServerStream[Req, Resp]) Recv() (*Req, error) {
	if len(s.reqs) == 0 {
		return nil, io.EOF
	}
	req := s.reqs[0]
	s.reqs = s.reqs[1:]
	return req, nil
}

func (s *fakeServerStream[Req, Resp]) Send(resp *Resp) error {
	s.sent = append(s.sent, resp)
	return nil
}

func TestStoreHooksAreReached(t *testing.T) {
	ctx := context.Background()
	reached := make(reachedHooks)
	requireReached := func(hooks ...string) {
		t.Helper()
		for _, h := range hooks {
			require.True(t, reached[h], h)
		}
		clear(reached)
	}
	refused := errors.New("refused")
	savedClient, savedFormat, savedWith, savedRefusal, savedLocator := checkClient, checkClientRepoFormat, withClientRepoFormat, addRefusal, newChunkLocator
	t.Cleanup(func() {
		checkClient, checkClientRepoFormat, withClientRepoFormat, addRefusal, newChunkLocator = savedClient, savedFormat, savedWith, savedRefusal, savedLocator
	})
	checkClient = func(RemoteSrvStore, []remotesapi.ClientCapability) error {
		reached["checkClient"] = true
		return nil
	}
	checkClientRepoFormat = func(*remotesapi.ClientRepoFormat) error {
		reached["checkClientRepoFormat"] = true
		return nil
	}
	withClientRepoFormat = func(ctx context.Context, _ *remotesapi.ClientRepoFormat) context.Context {
		reached["withClientRepoFormat"] = true
		return ctx
	}
	addRefusal = func(error) error {
		reached["addRefusal"] = true
		return refused
	}
	newChunkLocator = func() chunkLocator { return recordingLocator{reached} }

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
	rs := NewHttpFSBackedChunkStore(logrus.NewEntry(logrus.New()), "localhost", singleStoreDBCache{cs}, fs, "http", 0, identitySealer{}, nil)
	h := c.Hash()

	_, err = rs.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{RepoPath: "repo", ClientRepoFormat: &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}})
	require.NoError(t, err)
	requireReached("checkClientRepoFormat", "withClientRepoFormat")

	_, err = rs.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{RepoPath: "repo", ChunkHashes: [][]byte{h[:]}})
	require.NoError(t, err)
	requireReached("checkClient", "locate", "addRangeKeys")

	downloads := &fakeServerStream[remotesapi.GetDownloadLocsRequest, remotesapi.GetDownloadLocsResponse]{reqs: []*remotesapi.GetDownloadLocsRequest{{RepoPath: "repo", ChunkHashes: [][]byte{h[:]}}}}
	require.NoError(t, rs.StreamDownloadLocations(downloads))
	require.Len(t, downloads.sent, 1)
	requireReached("checkClient", "locate", "addRangeKeys")

	locations := &fakeServerStream[remotesapi.StreamChunkLocationsRequest, remotesapi.StreamChunkLocationsResponse]{reqs: []*remotesapi.StreamChunkLocationsRequest{{RepoPath: "repo", ChunkHashes: h[:]}}}
	require.NoError(t, rs.StreamChunkLocations(locations))
	require.Len(t, locations.sent, 1)
	requireReached("checkClient", "locate", "addLocationKeys")

	_, err = rs.ListTableFiles(ctx, &remotesapi.ListTableFilesRequest{RepoPath: "repo"})
	require.NoError(t, err)
	requireReached("checkClient")

	missing := hash.Of([]byte("missing"))
	tableInfo := []*remotesapi.ChunkTableInfo{{Hash: missing[:], ChunkCount: 1}}
	_, err = rs.AddTableFiles(ctx, &remotesapi.AddTableFilesRequest{RepoPath: "repo", ChunkTableInfo: tableInfo})
	require.ErrorIs(t, err, refused)
	requireReached("addRefusal")

	_, err = rs.Commit(ctx, &remotesapi.CommitRequest{RepoPath: "repo", Current: h[:], Last: h[:], ChunkTableInfo: tableInfo})
	require.ErrorIs(t, err, refused)
	requireReached("addRefusal")
}
