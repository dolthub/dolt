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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// clientFormatOfFake is what the fake format's clientFormat returns, so a request shows that it was reached.
var clientFormatOfFake = &remotesapi.ClientRepoFormat{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}

type recordingFormat struct {
	mu      *sync.Mutex
	reached map[string]bool
	key     *[]byte
}

func newRecordingFormat() recordingFormat {
	return recordingFormat{mu: new(sync.Mutex), reached: make(map[string]bool), key: &[]byte{1}}
}

func (f recordingFormat) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reached[call] = true
}

func (f recordingFormat) requireReached(t *testing.T, calls ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range calls {
		require.True(t, f.reached[c], c)
	}
}

func (f recordingFormat) clientFormat(*remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	f.record("clientFormat")
	return clientFormatOfFake
}

func (f recordingFormat) opened(*remotesapi.GetRepoMetadataResponse) (repoFormat, error) {
	f.record("opened")
	return f, nil
}

func (f recordingFormat) writeChunks(ctx context.Context, chnks []chunks.Chunk) (string, []byte, uint64, error) {
	f.record("writeChunks")
	return plainFormat{}.writeChunks(ctx, chnks)
}

func (f recordingFormat) newStreamKeys() streamKeys {
	return func([]*remotesapi.DownloadLoc, *remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse, map[uint32]*tableFileRec) error {
		f.record("streamKeys")
		return nil
	}
}

func (f recordingFormat) rangeKeys(hgr *remotesapi.HttpGetRange) ([]rangeKeys, error) {
	keys := make([]rangeKeys, len(hgr.Ranges))
	for i := range keys {
		keys[i].key = f.key
	}
	return keys, nil
}

func (f recordingFormat) wrap(cb func(context.Context, []byte, *Range) error, _ fetchResp, _ chan nbs.ToChunker) func(context.Context, []byte, *Range) error {
	f.record("wrap")
	return cb
}

type metadataClient struct {
	remotesapi.ChunkStoreServiceClient
	formats map[string]*remotesapi.ClientRepoFormat
}

func (c metadataClient) GetRepoMetadata(_ context.Context, req *remotesapi.GetRepoMetadataRequest, _ ...grpc.CallOption) (*remotesapi.GetRepoMetadataResponse, error) {
	c.formats["GetRepoMetadata"] = req.ClientRepoFormat
	return &remotesapi.GetRepoMetadataResponse{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}, nil
}

func (metadataClient) Root(context.Context, *remotesapi.RootRequest, ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	return &remotesapi.RootResponse{RootHash: make([]byte, hash.ByteLen)}, nil
}

func (metadataClient) GetUploadLocations(context.Context, *remotesapi.GetUploadLocsRequest, ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "no uploads")
}

func (c metadataClient) Commit(_ context.Context, req *remotesapi.CommitRequest, _ ...grpc.CallOption) (*remotesapi.CommitResponse, error) {
	c.formats["Commit"] = req.ClientRepoFormat
	return &remotesapi.CommitResponse{Success: true}, nil
}

func (c metadataClient) AddTableFiles(_ context.Context, req *remotesapi.AddTableFilesRequest, _ ...grpc.CallOption) (*remotesapi.AddTableFilesResponse, error) {
	c.formats["AddTableFiles"] = req.ClientRepoFormat
	return &remotesapi.AddTableFilesResponse{Success: true}, nil
}

func TestRepoFormatIsReached(t *testing.T) {
	ctx := context.Background()
	f := newRecordingFormat()
	client := metadataClient{formats: make(map[string]*remotesapi.ClientRepoFormat)}
	dcs, err := NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "org/repo", "host", false, client, func(o *chunkStoreOptions) {
		o.format = f
	})
	require.NoError(t, err)
	f.requireReached(t, "opened")

	c := chunks.NewChunk([]byte("upload"))
	_, err = dcs.uploadChunks(ctx, map[hash.Hash]nbs.CompressedChunk{c.Hash(): nbs.ChunkToCompressedChunk(c)})
	require.Error(t, err)
	f.requireReached(t, "writeChunks")

	_, err = dcs.Commit(ctx, hash.Hash{}, hash.Hash{})
	require.NoError(t, err)
	require.NoError(t, dcs.AddTableFilesToManifest(ctx, map[string]int{c.Hash().String(): 1}, nil))
	for _, rpc := range []string{"GetRepoMetadata", "Commit", "AddTableFiles"} {
		require.Same(t, clientFormatOfFake, client.formats[rpc], rpc)
	}
}

func TestRepoFormatIsReachedByFetches(t *testing.T) {
	f := newRecordingFormat()
	h := startFormatRPCThread(t, f)
	h.reqCh <- &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	<-h.stream.sentCh
	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "http://host/f-1", FileId: "f-1"}},
		Locations:  []*chunkLoc{{TableFileId: 1, RequestIndex: 0, Offset: 0, Length: 10}},
	}}
	locs := recvLocs(t, h.resCh)
	f.requireReached(t, "streamKeys")

	ctx := context.Background()
	locCh := make(chan []*remotesapi.DownloadLoc, 1)
	locCh <- locs
	close(locCh)
	fetchReqCh := make(chan fetchReq)
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- fetcherDownloadRangesThread(ctx, locCh, fetchReqCh, doneCh, f) }()
	req := fetchReq{respCh: make(chan fetchResp, 1)}
	fetchReqCh <- req
	resp := <-req.respCh
	require.NoError(t, <-errCh)
	require.Len(t, resp.get.Ranges, 1)
	require.Same(t, f.key, resp.get.Ranges[0].key)

	urlReqCh := make(chan fetchReq)
	urlDoneCh := make(chan struct{})
	go func() {
		req := <-urlReqCh
		req.respCh <- fetchResp{get: &GetRange{}, rangeType: rangeType_Chunk}
		close(urlDoneCh)
	}()
	require.NoError(t, fetcherDownloadURLThread(ctx, urlReqCh, urlDoneCh, nil, nil, nil, nil, nil, defaultRequestParams, func(string, ...interface{}) {}, f))
	f.requireReached(t, "wrap")
}
