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
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func TestTranslateChunkLocationsKeys(t *testing.T) {
	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(4)}
	tfByID := map[uint32]*tableFileRec{1: {TableFileId: 1, Url: "http://a"}, 2: {TableFileId: 2, Url: "http://b"}}
	keysByID := map[uint32][]byte{7: []byte("x"), 9: []byte("y")}
	locations := []*chunkLoc{
		{TableFileId: 1, RequestIndex: 0, KeyId: 9},
		{TableFileId: 2, RequestIndex: 1, KeyId: 7, DictionaryKeyId: 9},
		{TableFileId: 1, RequestIndex: 2, KeyId: 7, DictionaryKeyId: 9},
		{TableFileId: 1, RequestIndex: 3},
	}
	locs, err := translateChunkLocations(req, locations, tfByID, keysByID, testIdFunc, "repo")
	require.NoError(t, err)
	require.Len(t, locs, 2)

	a := locs[0].GetHttpGetRange()
	assert.Equal(t, [][]byte{[]byte("y"), []byte("x")}, a.Keys, "each range numbers its own keys")
	assert.Equal(t, []uint32{1, 2, 0}, []uint32{a.Ranges[0].KeyIndex, a.Ranges[1].KeyIndex, a.Ranges[2].KeyIndex})
	assert.Equal(t, []uint32{0, 1, 0}, []uint32{a.Ranges[0].DictionaryKeyIndex, a.Ranges[1].DictionaryKeyIndex, a.Ranges[2].DictionaryKeyIndex})
	b := locs[1].GetHttpGetRange()
	assert.Equal(t, [][]byte{[]byte("x"), []byte("y")}, b.Keys)
	assert.Equal(t, uint32(1), b.Ranges[0].KeyIndex)
	assert.Equal(t, uint32(2), b.Ranges[0].DictionaryKeyIndex)

	_, err = translateChunkLocations(req, []*chunkLoc{{TableFileId: 1, KeyId: 8}}, tfByID, keysByID, testIdFunc, "repo")
	assert.ErrorContains(t, err, "unknown key_id 8")
}

func TestDownloadsAddKeys(t *testing.T) {
	h := hash.Of([]byte("chunk"))
	loc := func(keys [][]byte, keyIndex, dictKeyIndex uint32) *remotesapi.DownloadLoc {
		return &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: &remotesapi.HttpGetRange{
			Url:  "http://example.com/file",
			Keys: keys,
			Ranges: []*remotesapi.RangeChunk{
				{Hash: h[:], Offset: 100, Length: 10, DictionaryOffset: 0, DictionaryLength: 50, KeyIndex: keyIndex, DictionaryKeyIndex: dictKeyIndex},
			},
		}}}
	}

	d := newDownloads()
	require.NoError(t, d.Add(loc([][]byte{[]byte("k"), []byte("dk")}, 1, 2)))
	dict := toGetRange(d.dictRanges.DeleteMaxRegion())
	require.Len(t, dict.Ranges, 1)
	assert.Equal(t, []byte("dk"), dict.Ranges[0].Key)
	chunk := toGetRange(d.chunkRanges.DeleteMaxRegion())
	require.Len(t, chunk.Ranges, 1)
	assert.Equal(t, []byte("k"), chunk.Ranges[0].Key)

	d = newDownloads()
	require.NoError(t, d.Add(loc(nil, 0, 0)))
	assert.Nil(t, toGetRange(d.chunkRanges.DeleteMaxRegion()).Ranges[0].Key)

	assert.ErrorContains(t, newDownloads().Add(loc([][]byte{[]byte("k")}, 2, 0)), "record key 2 of 1")
	assert.ErrorContains(t, newDownloads().Add(loc([][]byte{[]byte("k")}, 1, 3)), "record key 3 of 1")
}

func TestPlainFormatRefusesKeys(t *testing.T) {
	ctx := context.Background()
	c := chunks.NewChunk([]byte("chunk"))
	cc := nbs.ChunkToCompressedChunk(c)
	_, err := PlainFormat{}.OpenChunk(ctx, c.Hash(), []byte("k"), cc.FullCompressedChunk, nil)
	assert.ErrorIs(t, err, errUnexpectedKey)
	_, err = PlainFormat{}.OpenDictionary(ctx, []byte("k"), nil)
	assert.ErrorIs(t, err, errUnexpectedKey)

	tc, err := PlainFormat{}.OpenChunk(ctx, c.Hash(), nil, cc.FullCompressedChunk, nil)
	require.NoError(t, err)
	got, err := tc.ToChunk()
	require.NoError(t, err)
	assert.Equal(t, c.Data(), got.Data())
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
	assert.Equal(t, types.Format_DOLT.VersionString(), client.reqs[len(client.reqs)-1].ClientRepoFormat.NbfVersion)
}
