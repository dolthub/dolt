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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type recordingFormat struct {
	mu    *sync.Mutex
	calls *[]string
}

func newRecordingFormat() recordingFormat {
	return recordingFormat{mu: new(sync.Mutex), calls: new([]string)}
}

func (f recordingFormat) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	*f.calls = append(*f.calls, call)
}

func (f recordingFormat) took() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := *f.calls
	*f.calls = nil
	return ret
}

func (f recordingFormat) clientFormat(cf *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	f.record("clientFormat")
	return cf
}

func (f recordingFormat) opened(*remotesapi.GetRepoMetadataResponse) (repoFormat, error) {
	f.record("opened")
	return f, nil
}

func (f recordingFormat) writeChunks(chnks []chunks.Chunk) (string, []byte, uint64, error) {
	f.record("writeChunks")
	return storedFormat{}.writeChunks(chnks)
}

func (f recordingFormat) addLocationKeys([]*remotesapi.DownloadLoc, *remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse, map[uint32]*tableFileRec, map[uint32][]byte) error {
	f.record("addLocationKeys")
	return nil
}

func (f recordingFormat) addRangeKeys(downloads, *remotesapi.DownloadLoc) error {
	f.record("addRangeKeys")
	return nil
}

func (f recordingFormat) wrap(cb func(context.Context, []byte, *Range) error, _ fetchResp, _ chan nbs.ToChunker) func(context.Context, []byte, *Range) error {
	f.record("wrap")
	return cb
}

var errNoUploads = errors.New("no uploads")

type metadataClient struct {
	remotesapi.ChunkStoreServiceClient
}

func (metadataClient) GetRepoMetadata(context.Context, *remotesapi.GetRepoMetadataRequest, ...grpc.CallOption) (*remotesapi.GetRepoMetadataResponse, error) {
	return &remotesapi.GetRepoMetadataResponse{NbfVersion: types.Format_DOLT.VersionString(), NbsVersion: nbs.StorageVersion}, nil
}

func (metadataClient) Root(context.Context, *remotesapi.RootRequest, ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	return &remotesapi.RootResponse{RootHash: make([]byte, hash.ByteLen)}, nil
}

func (metadataClient) GetUploadLocations(context.Context, *remotesapi.GetUploadLocsRequest, ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	return nil, errNoUploads
}

func TestRepoFormatIsReached(t *testing.T) {
	ctx := context.Background()
	f := newRecordingFormat()
	dcs, err := NewDoltChunkStoreFromPath(ctx, types.Format_DOLT, "org/repo", "host", false, metadataClient{}, func(o *chunkStoreOptions) {
		o.format = f
	})
	require.NoError(t, err)
	require.Equal(t, []string{"clientFormat", "opened"}, f.took())

	require.NoError(t, dcs.refreshRepoMetadata(ctx))
	require.Equal(t, []string{"clientFormat", "opened"}, f.took())

	c := chunks.NewChunk([]byte("upload"))
	_, err = dcs.uploadChunks(ctx, map[hash.Hash]nbs.CompressedChunk{c.Hash(): nbs.ChunkToCompressedChunk(c)})
	require.Error(t, err)
	require.Equal(t, []string{"writeChunks"}, f.took())
}

func TestRepoFormatIsReachedByFetches(t *testing.T) {
	f := newRecordingFormat()
	h := startRPCThread(t, f)
	h.reqCh <- &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	<-h.stream.sentCh
	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "http://host/f-1", FileId: "f-1"}},
		Locations:  []*chunkLoc{{TableFileId: 1, RequestIndex: 0, Offset: 0, Length: 10}},
	}}
	locs := recvLocs(t, h.resCh)
	require.Equal(t, []string{"addLocationKeys"}, f.took())

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
	<-req.respCh
	require.NoError(t, <-errCh)
	require.Equal(t, []string{"addRangeKeys"}, f.took())

	urlReqCh := make(chan fetchReq)
	urlDoneCh := make(chan struct{})
	go func() {
		req := <-urlReqCh
		req.respCh <- fetchResp{get: &GetRange{}, rangeType: rangeType_Chunk}
		close(urlDoneCh)
	}()
	require.NoError(t, fetcherDownloadURLThread(ctx, urlReqCh, urlDoneCh, nil, nil, nil, nil, nil, defaultRequestParams, func(string, ...interface{}) {}, f))
	require.Equal(t, []string{"wrap"}, f.took())
}
