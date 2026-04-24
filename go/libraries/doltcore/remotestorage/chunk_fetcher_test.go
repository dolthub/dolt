// Copyright 2024 Dolthub, Inc.
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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

func TestFetcherHashSetToReqsThread(t *testing.T) {
	t.Run("ImmediateClose", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		close(reqCh)

		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		err := fetcherHashSetToReqsThread(context.Background(), reqCh, nil, resCh, 32, testBuildDlReq)
		assert.NoError(t, err)
		_, ok := <-resCh
		assert.False(t, ok)
	})

	t.Run("CanceledContext", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fetcherHashSetToReqsThread(ctx, reqCh, nil, resCh, 32, testBuildDlReq)
		assert.Error(t, err)
	})

	t.Run("BatchesAsExpected", func(t *testing.T) {
		reqCh := make(chan hash.HashSet)
		resCh := make(chan *remotesapi.GetDownloadLocsRequest)

		eg, ctx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return fetcherHashSetToReqsThread(ctx, reqCh, nil, resCh, 8, testBuildDlReq)
		})

		// First send a batch of 16 hashes.
		{
			hs := make(hash.HashSet)
			for i := 0; i < 16; i++ {
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
			}
			reqCh <- hs
		}
		// And read the two requests that get formed...
		apiReq := <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)

		// Next send 12 batches of one...
		{
			for i := 0; i < 12; i++ {
				hs := make(hash.HashSet)
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
				reqCh <- hs
			}
		}
		// Read one batch of 8...
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)

		// Send 8 more batches of one...
		{
			for i := 12; i < 20; i++ {
				hs := make(hash.HashSet)
				var h hash.Hash
				h[0] = byte(i)
				hs.Insert(h)
				reqCh <- hs
			}
		}
		// Read a batch of 8 and a batch of 4...
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 8)
		apiReq = <-resCh
		assert.NotNil(t, apiReq)
		assert.Len(t, apiReq.ChunkHashes, 4)

		close(reqCh)
		assert.NoError(t, eg.Wait())
	})
}

func testIdFunc() (*remotesapi.RepoId, string) {
	return new(remotesapi.RepoId), ""
}

func testBuildDlReq(hashes [][]byte) *remotesapi.GetDownloadLocsRequest {
	id, token := testIdFunc()
	return &remotesapi.GetDownloadLocsRequest{RepoId: id, RepoToken: token, ChunkHashes: hashes}
}

// buildFlatHashBuf returns a |nHashes|*20-byte buffer where hash |i|
// has byte[0] = i+1 as a distinct marker. Used by tests that need to
// assert a ChunkLocation.RequestIndex correctly decodes into the
// request's chunk_hashes buffer.
func buildFlatHashBuf(nHashes int) []byte {
	buf := make([]byte, hash.ByteLen*nHashes)
	for i := 0; i < nHashes; i++ {
		buf[i*hash.ByteLen] = byte(i + 1)
	}
	return buf
}

func TestHashAtIndex(t *testing.T) {
	buf := buildFlatHashBuf(3)

	t.Run("InRange", func(t *testing.T) {
		bs, ok := hashAtIndex(buf, 0)
		require.True(t, ok)
		assert.Len(t, bs, hash.ByteLen)
		assert.Equal(t, byte(1), bs[0])

		bs, ok = hashAtIndex(buf, 2)
		require.True(t, ok)
		assert.Equal(t, byte(3), bs[0])
	})
	t.Run("OutOfRange", func(t *testing.T) {
		_, ok := hashAtIndex(buf, 3)
		assert.False(t, ok)
		_, ok = hashAtIndex(buf, 100)
		assert.False(t, ok)
	})
	t.Run("EmptyBuffer", func(t *testing.T) {
		_, ok := hashAtIndex(nil, 0)
		assert.False(t, ok)
		_, ok = hashAtIndex([]byte{}, 0)
		assert.False(t, ok)
	})
}

func TestTranslateChunkLocations(t *testing.T) {
	repoId := &remotesapi.RepoId{Org: "dolthub", RepoName: "repo"}
	getRepoId := func() (*remotesapi.RepoId, string) {
		return repoId, "token"
	}
	const repoPath = "dolthub/repo"

	buildReq := func(nHashes int) *remotesapi.StreamChunkLocationsRequest {
		return &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(nHashes)}
	}

	t.Run("EmptyLocations", func(t *testing.T) {
		locs, err := translateChunkLocations(buildReq(0), nil, nil, getRepoId, repoPath)
		require.NoError(t, err)
		assert.Nil(t, locs)
	})

	t.Run("SameResponseRecord", func(t *testing.T) {
		req := buildReq(2)
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "https://example.com/tf-1", FileId: "file-1"},
		}
		locations := []*chunkLoc{
			{TableFileId: 1, RequestIndex: 0, Offset: 0, Length: 10},
			{TableFileId: 1, RequestIndex: 1, Offset: 10, Length: 20},
		}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		hgr := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		assert.Equal(t, "https://example.com/tf-1", hgr.Url)
		require.Len(t, hgr.Ranges, 2)
		require.NotNil(t, locs[0].RefreshRequest)
		assert.Equal(t, repoId, locs[0].RefreshRequest.RepoId)
		assert.Equal(t, "token", locs[0].RefreshRequest.RepoToken)
		assert.Equal(t, repoPath, locs[0].RefreshRequest.RepoPath)
		assert.Equal(t, "file-1", locs[0].RefreshRequest.FileId)
	})

	t.Run("CrossResponseReuse", func(t *testing.T) {
		// tfByID carries an entry from an earlier response; this
		// response references it without a fresh TableFileRecord.
		req := buildReq(1)
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "urlA", FileId: "file-1"},
		}
		locations := []*chunkLoc{{TableFileId: 1, RequestIndex: 0}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		hgr := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		assert.Equal(t, "urlA", hgr.Url)
	})

	t.Run("OverwriteSimulation", func(t *testing.T) {
		// Simulates a reconnect: the RPC thread has integrated a
		// fresh TableFileRecord{id=1, url=urlB} into tfByID,
		// overwriting an earlier urlA. Subsequent translation must
		// use urlB.
		req := buildReq(1)
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "urlB", FileId: "file-1-b"},
		}
		locations := []*chunkLoc{{TableFileId: 1, RequestIndex: 0}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		hgr := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		assert.Equal(t, "urlB", hgr.Url)
		assert.Equal(t, "file-1-b", locs[0].RefreshRequest.FileId)
	})

	t.Run("ProtocolViolationUnknownTableFileId", func(t *testing.T) {
		req := buildReq(1)
		tfByID := map[uint32]*tableFileRec{}
		locations := []*chunkLoc{{TableFileId: 42, RequestIndex: 0}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		assert.Nil(t, locs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table_file_id 42")
	})

	t.Run("ProtocolViolationRequestIndexOutOfRange", func(t *testing.T) {
		req := buildReq(2) // valid indices: 0 and 1
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "u"},
		}
		locations := []*chunkLoc{{TableFileId: 1, RequestIndex: 2}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		assert.Nil(t, locs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})

	t.Run("MultipleTableFilesPreservesFirstSeenOrder", func(t *testing.T) {
		req := buildReq(3)
		tfByID := map[uint32]*tableFileRec{
			5: {TableFileId: 5, Url: "url-5"},
			7: {TableFileId: 7, Url: "url-7"},
		}
		// First-seen order in locations: id=7, then id=5.
		locations := []*chunkLoc{
			{TableFileId: 7, RequestIndex: 0},
			{TableFileId: 5, RequestIndex: 1},
			{TableFileId: 7, RequestIndex: 2},
		}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 2)

		hgr0 := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		assert.Equal(t, "url-7", hgr0.Url)
		assert.Len(t, hgr0.Ranges, 2)

		hgr1 := locs[1].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		assert.Equal(t, "url-5", hgr1.Url)
		assert.Len(t, hgr1.Ranges, 1)
	})

	t.Run("RefreshAfterPropagated", func(t *testing.T) {
		req := buildReq(1)
		now := timestamppb.Now()
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "u", FileId: "f", RefreshAfter: now},
		}
		locations := []*chunkLoc{{TableFileId: 1, RequestIndex: 0}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		assert.Equal(t, now, locs[0].RefreshAfter)
	})

	t.Run("RangeChunkHashDecodedFromRequestBuffer", func(t *testing.T) {
		// RequestIndex=2 should pull bytes [40:60] from the
		// flat 3*20-byte buffer, where byte[0]=3.
		req := buildReq(3)
		tfByID := map[uint32]*tableFileRec{
			1: {TableFileId: 1, Url: "u"},
		}
		locations := []*chunkLoc{{TableFileId: 1, RequestIndex: 2, Offset: 0, Length: 10}}
		locs, err := translateChunkLocations(req, locations, tfByID, getRepoId, repoPath)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		hgr := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		require.Len(t, hgr.Ranges, 1)
		assert.Len(t, hgr.Ranges[0].Hash, hash.ByteLen)
		assert.Equal(t, byte(3), hgr.Ranges[0].Hash[0])
	})
}

// fakeStreamChunkLocationsClient is a minimal in-memory fake of the
// generated ChunkStoreService_StreamChunkLocationsClient interface.
// The reliable-RPC layer only calls Send/Recv/CloseSend; the
// embedded grpc.ClientStream is nil and unused.
type fakeStreamChunkLocationsClient struct {
	grpc.ClientStream

	ctx    context.Context
	sentCh chan *remotesapi.StreamChunkLocationsRequest
	recvCh chan fakeStreamResult
}

type fakeStreamResult struct {
	resp *remotesapi.StreamChunkLocationsResponse
	err  error
}

func newFakeStream(ctx context.Context) *fakeStreamChunkLocationsClient {
	return &fakeStreamChunkLocationsClient{
		ctx:    ctx,
		sentCh: make(chan *remotesapi.StreamChunkLocationsRequest, 64),
		recvCh: make(chan fakeStreamResult, 64),
	}
}

func (s *fakeStreamChunkLocationsClient) Send(req *remotesapi.StreamChunkLocationsRequest) error {
	select {
	case s.sentCh <- req:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *fakeStreamChunkLocationsClient) Recv() (*remotesapi.StreamChunkLocationsResponse, error) {
	select {
	case r, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return r.resp, r.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeStreamChunkLocationsClient) CloseSend() error { return nil }

// fakeCSClient is just enough of remotesapi.ChunkStoreServiceClient to
// open the StreamChunkLocations stream under test. Other methods
// inherit nil behavior from the embedded interface and will panic if
// called (which they aren't, by the RPC thread).
type fakeCSClient struct {
	remotesapi.ChunkStoreServiceClient
	stream *fakeStreamChunkLocationsClient
}

func (c *fakeCSClient) StreamChunkLocations(ctx context.Context, opts ...grpc.CallOption) (remotesapi.ChunkStoreService_StreamChunkLocationsClient, error) {
	return c.stream, nil
}

// rpcTestHarness wraps fetcherRPCChunkLocationsThread and the fake
// stream so individual tests can drive requests and responses.
type rpcTestHarness struct {
	t         *testing.T
	cancel    context.CancelFunc
	stream    *fakeStreamChunkLocationsClient
	reqCh     chan *remotesapi.StreamChunkLocationsRequest
	resCh     chan []*remotesapi.DownloadLoc
	missingCh chan nbs.ToChunker

	// done closes when the RPC thread has exited; finalErr holds its
	// terminal error. Read via wait().
	done     chan struct{}
	finalErr error
}

func startRPCThread(t *testing.T) *rpcTestHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx)
	client := &fakeCSClient{stream: stream}

	h := &rpcTestHarness{
		t:         t,
		cancel:    cancel,
		stream:    stream,
		reqCh:     make(chan *remotesapi.StreamChunkLocationsRequest, 16),
		resCh:     make(chan []*remotesapi.DownloadLoc, 16),
		missingCh: make(chan nbs.ToChunker, 16),
		done:      make(chan struct{}),
	}
	repoId := &remotesapi.RepoId{Org: "org", RepoName: "repo"}
	getRepoId := func() (*remotesapi.RepoId, string) { return repoId, "tok" }
	go func() {
		defer close(h.done)
		h.finalErr = fetcherRPCChunkLocationsThread(ctx, h.reqCh, h.resCh, client, func(string) {}, h.missingCh, "host", getRepoId, "repoPath")
	}()

	// Backstop: make sure the RPC goroutine is torn down no matter
	// how the test ends (pass, fail, require.* abort, panic). If the
	// test already drove a clean shutdown, h.done is already closed
	// and this is a no-op. Otherwise cancelling the context unblocks
	// Send/Recv on the fake stream and the reliable-layer goroutines
	// so the RPC thread exits before the next test starts.
	t.Cleanup(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Errorf("RPC thread did not exit within 5s of cleanup")
		}
	})
	return h
}

// wait blocks until the RPC thread has exited and returns its
// terminal error. Typical use:
//   - Happy-path tests close h.reqCh and h.stream.recvCh to drive a
//     clean EOF shutdown, then assert require.NoError(t, h.wait()).
//   - Error-path tests inject an error response, then check
//     require.Error(t, h.wait()).
//
// Times out so a stuck test fails fast rather than hanging the suite.
func (h *rpcTestHarness) wait() error {
	h.t.Helper()
	select {
	case <-h.done:
		return h.finalErr
	case <-time.After(5 * time.Second):
		h.cancel()
		h.t.Fatal("RPC thread did not exit within 5s of wait()")
		return nil
	}
}

func recvLocs(t *testing.T, ch <-chan []*remotesapi.DownloadLoc) []*remotesapi.DownloadLoc {
	t.Helper()
	select {
	case locs := <-ch:
		return locs
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for DownloadLocs on resCh")
		return nil
	}
}

func recvMissing(t *testing.T, ch <-chan nbs.ToChunker) nbs.CompressedChunk {
	t.Helper()
	select {
	case c := <-ch:
		cc, ok := c.(nbs.CompressedChunk)
		require.True(t, ok, "expected nbs.CompressedChunk on missingCh")
		return cc
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for missing chunk on missingCh")
		return nbs.CompressedChunk{}
	}
}

func TestFetcherRPCChunkLocationsThread_HappyPath(t *testing.T) {
	h := startRPCThread(t)

	// Send a request with 2 hashes; the stream should see it
	// after the reliable layer forwards it.
	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(2)}
	h.reqCh <- req

	select {
	case sent := <-h.stream.sentCh:
		assert.Equal(t, req, sent)
	case <-time.After(2 * time.Second):
		t.Fatal("Send never happened on the fake stream")
	}

	// Push a response that introduces table_file_id=1 and places
	// both hashes in it.
	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "url-1", FileId: "f-1"}},
		Locations: []*chunkLoc{
			{TableFileId: 1, RequestIndex: 0, Offset: 0, Length: 10},
			{TableFileId: 1, RequestIndex: 1, Offset: 100, Length: 20},
		},
	}}

	locs := recvLocs(t, h.resCh)
	require.Len(t, locs, 1)
	hgr := locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
	assert.Equal(t, "url-1", hgr.Url)
	assert.Len(t, hgr.Ranges, 2)

	close(h.reqCh)
	close(h.stream.recvCh)
	require.NoError(t, h.wait())
}

func TestFetcherRPCChunkLocationsThread_CrossResponseReuse(t *testing.T) {
	h := startRPCThread(t)

	// Request 1.
	req1 := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req1
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "url-1"}},
		Locations:  []*chunkLoc{{TableFileId: 1, RequestIndex: 0}},
	}}

	locs := recvLocs(t, h.resCh)
	require.Len(t, locs, 1)
	assert.Equal(t, "url-1", locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url)

	// Request 2: server references id=1 without re-sending the
	// TableFileRecord. The long-lived tfByID map in the RPC thread
	// must still resolve it.
	req2 := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req2
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		Locations: []*chunkLoc{{TableFileId: 1, RequestIndex: 0}},
	}}

	locs = recvLocs(t, h.resCh)
	require.Len(t, locs, 1)
	assert.Equal(t, "url-1", locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url)

	close(h.reqCh)
	close(h.stream.recvCh)
	require.NoError(t, h.wait())
}

func TestFetcherRPCChunkLocationsThread_OverwriteSemantics(t *testing.T) {
	// Two responses, same table_file_id, different URLs. The second
	// TableFileRecord must overwrite the first so subsequent
	// ChunkLocations resolve to urlB. This is the "reconnect
	// simulation" — a fresh server handler re-uses id=1 with a new
	// URL, which the client must handle transparently.
	h := startRPCThread(t)

	req1 := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req1
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "urlA"}},
		Locations:  []*chunkLoc{{TableFileId: 1, RequestIndex: 0}},
	}}

	locs := recvLocs(t, h.resCh)
	require.Len(t, locs, 1)
	assert.Equal(t, "urlA", locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url)

	req2 := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req2
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		TableFiles: []*tableFileRec{{TableFileId: 1, Url: "urlB"}},
		Locations:  []*chunkLoc{{TableFileId: 1, RequestIndex: 0}},
	}}

	locs = recvLocs(t, h.resCh)
	require.Len(t, locs, 1)
	assert.Equal(t, "urlB", locs[0].Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url)

	close(h.reqCh)
	close(h.stream.recvCh)
	require.NoError(t, h.wait())
}

func TestFetcherRPCChunkLocationsThread_MissingIndexes(t *testing.T) {
	h := startRPCThread(t)

	// Request with 3 hashes. Server returns no locations and lists
	// all three indices as missing.
	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(3)}
	h.reqCh <- req
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		MissingIndexes: []uint32{0, 2},
	}}

	// Expect two empty CompressedChunks with hashes decoded from
	// buf[0:20] and buf[40:60].
	missing0 := recvMissing(t, h.missingCh)
	missing1 := recvMissing(t, h.missingCh)

	var expect0, expect1 hash.Hash
	copy(expect0[:], req.ChunkHashes[0*hash.ByteLen:1*hash.ByteLen])
	copy(expect1[:], req.ChunkHashes[2*hash.ByteLen:3*hash.ByteLen])

	got := map[hash.Hash]bool{missing0.Hash(): true, missing1.Hash(): true}
	assert.True(t, got[expect0])
	assert.True(t, got[expect1])

	// The response is still delivered on resCh (with no locs) to
	// preserve the 1:1 req/resp ordering the reliable layer expects.
	locs := recvLocs(t, h.resCh)
	assert.Nil(t, locs)

	close(h.reqCh)
	close(h.stream.recvCh)
	require.NoError(t, h.wait())
}

func TestFetcherRPCChunkLocationsThread_ProtocolViolationUnknownTableFileId(t *testing.T) {
	h := startRPCThread(t)

	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req
	<-h.stream.sentCh

	// Server references id=42 without ever introducing it.
	h.stream.recvCh <- fakeStreamResult{resp: &remotesapi.StreamChunkLocationsResponse{
		Locations: []*chunkLoc{{TableFileId: 42, RequestIndex: 0}},
	}}

	err := h.wait()
	require.Error(t, err)
	// NewRpcError wraps the underlying message.
	assert.Contains(t, err.Error(), "table_file_id 42")
}

func TestFetcherRPCChunkLocationsThread_PermanentStreamError(t *testing.T) {
	// A permanent gRPC error (e.g. PermissionDenied) on Recv is
	// classified as non-retriable by processGrpcErr and should
	// surface from the thread directly rather than loop forever.
	h := startRPCThread(t)

	req := &remotesapi.StreamChunkLocationsRequest{ChunkHashes: buildFlatHashBuf(1)}
	h.reqCh <- req
	<-h.stream.sentCh

	h.stream.recvCh <- fakeStreamResult{err: status.Error(codes.PermissionDenied, "nope")}

	err := h.wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")
}
