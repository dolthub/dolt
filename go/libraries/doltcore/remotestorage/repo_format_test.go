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

package remotestorage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func TestRangeKeys(t *testing.T) {
	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(2)}
	tfByID := map[uint32]*tableFileRec{1: {TableFileId: 1, Url: "http://a"}}
	keysByID := map[uint32][]byte{7: []byte("x"), 9: []byte("y")}
	locs, err := translateChunkLocations(req, []*chunkLoc{
		{TableFileId: 1, RequestIndex: 0, Offset: 100, Length: 10, KeyId: 9},
		{TableFileId: 1, RequestIndex: 1, Offset: 200, Length: 10, DictionaryLength: 50, KeyId: 7, DictionaryKeyId: 9},
	}, tfByID, keysByID, testIdFunc, "repo")
	require.NoError(t, err)
	require.Len(t, locs, 1)
	hgr := locs[0].GetHttpGetRange()
	assert.Equal(t, [][]byte{[]byte("y"), []byte("x")}, hgr.Keys)

	d := newDownloads()
	require.NoError(t, d.Add(locs[0]))
	dict := toGetRange(d.dictRanges.DeleteMaxRegion())
	assert.Equal(t, []byte("y"), keyBytes(dict.Ranges[0].Key))
	var got [][]byte
	for _, r := range toGetRange(d.chunkRanges.DeleteMaxRegion()).Ranges {
		got = append(got, keyBytes(r.Key))
	}
	assert.Equal(t, [][]byte{[]byte("y"), []byte("x")}, got)

	_, err = translateChunkLocations(req, []*chunkLoc{{TableFileId: 1, KeyId: 8}}, tfByID, keysByID, testIdFunc, "repo")
	assert.ErrorContains(t, err, "unknown key_id 8")
	hgr.Ranges[0].KeyIndex = 3
	assert.ErrorContains(t, newDownloads().Add(locs[0]), "record key 3 of 2")
}

func TestPlainFormatRefusesKeys(t *testing.T) {
	ctx := context.Background()
	_, err := PlainFormat{}.OpenChunk(ctx, hash.Hash{}, []byte("k"), nil, nil)
	assert.ErrorIs(t, err, errUnexpectedKey)
	_, err = PlainFormat{}.OpenDictionary(ctx, []byte("k"), nil)
	assert.ErrorIs(t, err, errUnexpectedKey)
}

type metadataClient struct {
	remotesapi.ChunkStoreServiceClient
	md   *remotesapi.GetRepoMetadataResponse
	reqs []*remotesapi.GetRepoMetadataRequest
}

func (c *metadataClient) GetRepoMetadata(_ context.Context, req *remotesapi.GetRepoMetadataRequest, _ ...grpc.CallOption) (*remotesapi.GetRepoMetadataResponse, error) {
	c.reqs = append(c.reqs, req)
	return c.md, nil
}

func (c *metadataClient) Root(context.Context, *remotesapi.RootRequest, ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	return &remotesapi.RootResponse{RootHash: make([]byte, hash.ByteLen)}, nil
}

// toyRepoFormat asks for repositories of format "toy", and settles into |settled| when it opens one.
type toyRepoFormat struct {
	PlainFormat
	settled string
	opened  *[]string
}

var errToyFormat = errors.New("toy format changed")

func (f toyRepoFormat) ClientFormat(cf *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	cf.NbfVersion = "toy" + f.settled
	cf.EncryptionScope = []byte(f.settled)
	return cf
}

func (f toyRepoFormat) Opened(md *remotesapi.GetRepoMetadataResponse) (RepoFormat, error) {
	*f.opened = append(*f.opened, md.NbfVersion)
	if f.settled != "" && md.NbfVersion != f.settled {
		return nil, errToyFormat
	}
	f.settled = md.NbfVersion
	return f, nil
}

func TestRepoFormatOfDoltChunkStore(t *testing.T) {
	ctx := context.Background()
	client := &metadataClient{md: &remotesapi.GetRepoMetadataResponse{NbfVersion: "-a"}}
	var opened []string
	dcs, err := NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "org/repo", "host", false, client, toyRepoFormat{opened: &opened})
	require.NoError(t, err)
	require.Len(t, client.reqs, 1)
	assert.Equal(t, "toy", client.reqs[0].ClientRepoFormat.NbfVersion)
	assert.Equal(t, nbs.StorageVersion, client.reqs[0].ClientRepoFormat.NbsVersion)

	require.NoError(t, dcs.Rebase(ctx))
	require.Len(t, client.reqs, 2)
	assert.Equal(t, "toy-a", client.reqs[1].ClientRepoFormat.NbfVersion, "requests are sent in the settled format")
	assert.Equal(t, []byte("-a"), client.reqs[1].ClientRepoFormat.EncryptionScope)

	client.md = &remotesapi.GetRepoMetadataResponse{NbfVersion: "-b"}
	assert.ErrorIs(t, dcs.Rebase(ctx), errToyFormat)
	assert.Equal(t, "-a", dcs.Version(), "a refused refresh keeps the metadata")
	assert.Equal(t, []string{"-a", "-a", "-b"}, opened)

	_, err = NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "org/repo", "host", false, client, toyRepoFormat{settled: "-a", opened: &opened})
	assert.ErrorIs(t, err, errToyFormat)

	dcs, err = NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "org/repo", "host", false, client)
	require.NoError(t, err)
	assert.Equal(t, PlainFormat{}, dcs.format)
}
