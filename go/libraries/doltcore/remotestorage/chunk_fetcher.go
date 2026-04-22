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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/pool"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/ranges"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/reliable"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/netstats"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// protoChunksEnabled, when true, replaces the StreamChunkLocations +
// HTTP range-fetch pipeline with a single StreamChunks RPC that
// returns chunk bytes directly over gRPC. Prototype, env-gated on the
// client only. See FETCH_PERFORMANCE_NOTES.md Section E.
var protoChunksEnabled = os.Getenv("DOLT_PROTO_CHUNKS") != ""

// Local aliases for the deeply nested generated message types so the
// rest of this file stays readable.
type (
	tableFileRec = remotesapi.StreamChunkLocationsResponse_TableFileRecord
	chunkLoc     = remotesapi.StreamChunkLocationsResponse_ChunkLocation
)

// hashAtIndex returns the 20-byte sub-slice of |buf| at hash index
// |idx| (i.e. buf[idx*20 : (idx+1)*20]), or ok=false if |idx| is
// out of range for |buf| viewed as a flat 20-byte-per-hash buffer.
// Used to decode request_index / missing_indexes returned by the
// server against the request's chunk_hashes buffer.
func hashAtIndex(buf []byte, idx uint32) ([]byte, bool) {
	start := int(idx) * hash.ByteLen
	end := start + hash.ByteLen
	if end > len(buf) {
		return nil, false
	}
	return buf[start:end], true
}

// A remotestorage.ChunkFetcher is a pipelined chunk fetcher for fetching a
// large number of chunks where the downloads may benefit from range
// coalescing, hedging, automatic retries, pipelining of download location
// retrieval with the fetching of the actual chunk bytes, etc.
//
// It is expected that one goroutine will be calling `Get()` with batches of
// addresses to retrieve, and another goroutine will be calling `Recv()`,
// reading fetched chunks.
//
// When all addresses have been delivered, calling `CloseSend()` will
// eventually cause the `Recv()` thread to read an `io.EOF` error, which
// indicates that all requested chunks have been delivered.
type ChunkFetcher struct {
	eg    *errgroup.Group
	egCtx context.Context

	// toGetCh is the channel used to request chunks. This will be initially given a root,
	// and as refs are found, they will be added to the channel for workers to batch and request.
	toGetCh chan hash.HashSet

	// resCh is the results channel for the fetcher. It is used both to return
	// chunks themselves, and to indicate which chunks were requested but missing
	// by having a Hash, but are empty.
	resCh chan nbs.ToChunker

	abortCh chan struct{}
	stats   StatsRecorder
}

const (
	getLocsBatchSize = 512

	reliableCallReadRequestTimeout = 15 * time.Second
	reliableCallDeliverRespTimeout = 15 * time.Second
)

func NewChunkFetcher(ctx context.Context, dcs *DoltChunkStore) *ChunkFetcher {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &ChunkFetcher{
		eg:    eg,
		egCtx: ctx,

		toGetCh: make(chan hash.HashSet),
		resCh:   make(chan nbs.ToChunker),

		abortCh: make(chan struct{}),
		stats:   StatsFactory(),
	}

	storeRepoToken := func(s string) { dcs.repoToken.Store(s) }

	if protoChunksEnabled {
		getCh := make(chan *remotesapi.StreamChunksRequest_Get)
		eg.Go(func() error {
			return fetcherHashSetToGetsThread(ctx, ret.toGetCh, ret.abortCh, getCh)
		})
		eg.Go(func() error {
			return fetcherStreamChunksThreads(ctx, getCh, ret.resCh, dcs.csClient, dcs.getRepoId, dcs.repoPath, storeRepoToken, dcs.host)
		})
		return ret
	}

	downloadLocCh := make(chan []*remotesapi.DownloadLoc)
	locDoneCh := make(chan struct{})
	fetchReqCh := make(chan fetchReq)

	if hasFeature(dcs.metadata, remotesapi.Feature_FEATURE_STREAM_CHUNK_LOCATIONS) {
		locsReqCh := make(chan *remotesapi.StreamChunkLocationsRequest)
		eg.Go(func() error {
			return fetcherHashSetToReqsThread(ctx, ret.toGetCh, ret.abortCh, locsReqCh, getLocsBatchSize, func(hashes [][]byte) *remotesapi.StreamChunkLocationsRequest {
				// StreamChunkLocationsRequest.chunk_hashes is a
				// flat 20-byte-per-hash buffer, not repeated bytes
				// (see the proto file for rationale).
				flat := make([]byte, 0, hash.ByteLen*len(hashes))
				for _, h := range hashes {
					flat = append(flat, h...)
				}
				id, token := dcs.getRepoId()
				return &remotesapi.StreamChunkLocationsRequest{RepoId: id, RepoPath: dcs.repoPath, RepoToken: token, ChunkHashes: flat}
			})
		})
		eg.Go(func() error {
			return fetcherRPCChunkLocationsThread(ctx, locsReqCh, downloadLocCh, dcs.csClient, storeRepoToken, ret.resCh, dcs.host, dcs.getRepoId, dcs.repoPath)
		})
	} else {
		locsReqCh := make(chan *remotesapi.GetDownloadLocsRequest)
		eg.Go(func() error {
			return fetcherHashSetToReqsThread(ctx, ret.toGetCh, ret.abortCh, locsReqCh, getLocsBatchSize, func(hashes [][]byte) *remotesapi.GetDownloadLocsRequest {
				id, token := dcs.getRepoId()
				return &remotesapi.GetDownloadLocsRequest{RepoId: id, RepoPath: dcs.repoPath, RepoToken: token, ChunkHashes: hashes}
			})
		})
		eg.Go(func() error {
			return fetcherRPCDownloadLocsThread(ctx, locsReqCh, downloadLocCh, dcs.csClient, storeRepoToken, ret.resCh, dcs.host)
		})
	}
	eg.Go(func() error {
		return fetcherDownloadRangesThread(ctx, downloadLocCh, fetchReqCh, locDoneCh)
	})
	eg.Go(func() error {
		return fetcherDownloadURLThreads(ctx, fetchReqCh, locDoneCh, ret.resCh, dcs.csClient, ret.stats, dcs.httpFetcher, dcs.params)
	})

	return ret
}

// Implements nbs.ChunkFetcher. Request the contents the chunks the given
// |hashes|. They will be delivered through |Recv|. Returns an error if this
// ChunkFetcher is terminally failed or if the supplied |ctx| is |Done|.
func (f *ChunkFetcher) Get(ctx context.Context, hashes hash.HashSet) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.egCtx.Done():
		return context.Cause(f.egCtx)
	case f.toGetCh <- hashes:
		return nil
	}
}

// Imeplements nbs.ChunkFetcher. Indicates that no further hashes will be
// requested through |Get|. |Recv| will only return |io.EOF| after this is
// called.
func (f *ChunkFetcher) CloseSend() error {
	close(f.toGetCh)
	return nil
}

// Implements nbs.ChunkFetcher. Returns the next available
// |nbs.CompressedChunk| whose contents have been fetched after being requested
// by |Get|. Returns |io.EOF| after |CloseSend| is called and all requested
// chunks have been successfully received. Returns an error if this
// |ChunkFetcher| is terminally failed or if the supplied |ctx| is |Done|.
func (f *ChunkFetcher) Recv(ctx context.Context) (nbs.ToChunker, error) {
	select {
	case <-ctx.Done():
		return nbs.CompressedChunk{}, context.Cause(ctx)
	case <-f.egCtx.Done():
		return nbs.CompressedChunk{}, context.Cause(f.egCtx)
	case cc, ok := <-f.resCh:
		if !ok {
			return nbs.CompressedChunk{}, io.EOF
		}
		return cc, nil
	}
}

// Implements nbs.ChunkFetcher. Makes sure all resources associated with this
// ChunkFetcher are released, and returns any errors encountered while fetching
// requested chunks. This may return a non-|nil| error if the |ChunkFetcher| is
// |Close|d before it has delivered |io.EOF| on a |Recv| call, but that is not
// guaranteed. The correct way to guarantee everything has been received
// without error is to read |Recv| until it returns |io.EOF|, and then to
// |Close| the |ChunkFetcher|.
func (f *ChunkFetcher) Close() error {
	defer StatsFlusher(f.stats)
	close(f.abortCh)
	return f.eg.Wait()
}

// Reads HashSets from reqCh and batches all the received addresses
// into request messages with up to |batchSize| chunk hashes in them.
// |build| constructs the outbound request from a slice of wire-encoded
// hashes. Delivers the batched messages to |resCh|.
//
// Parameterized on the request type so the same batching logic works
// for both StreamDownloadLocations (legacy) and StreamChunkLocations
// (new), which share the same wire shape for requests.
func fetcherHashSetToReqsThread[R any](ctx context.Context, reqCh chan hash.HashSet, abortCh chan struct{}, resCh chan R, batchSize int, build func(chunkHashes [][]byte) R) error {
	// This is the buffer of received hashes that we haven't sent to |resCh| yet.
	var addrs [][]byte
	// This is the current slice we're trying to send in a request. After
	// we send it successfully, we will need to allocate a new one for the
	// next message, but we can reuse its memory when we fail to send on
	// |resCh| to form the next request we try to send.
	var outbound [][]byte
	for {
		if reqCh == nil && len(addrs) == 0 {
			close(resCh)
			return nil
		}

		var thisResCh chan R
		var thisRes R
		var thisLen int

		// Each time through the loop, we build a new request to send.
		// It contains up to |batchSize| hashes from the end of
		// |addrs|. If we successfully send it, then we will drop those
		// addresses from the end of |addrs|.
		if len(addrs) > 0 {
			end := len(addrs)
			st := end - batchSize
			if st < 0 {
				st = 0
			}
			if outbound == nil {
				outbound = make([][]byte, end-st)
			}
			outbound = append(outbound[:0], addrs[st:end]...)
			thisRes = build(outbound[:])
			thisLen = end - st
			thisResCh = resCh
		}

		select {
		case hs, ok := <-reqCh:
			if !ok {
				reqCh = nil
				break
			}
			for h := range hs {
				addrs = append(addrs, h[:])
			}
		case thisResCh <- thisRes:
			addrs = addrs[:len(addrs)-thisLen]
			outbound = nil
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-abortCh:
			return errors.New("early shutdown before all chunks fetched")
		}
	}
}

// Reads request messages off |reqCh| and sends them to a streaming RPC to turn
// them into download locations, which it delivers to |resCh|.
//
// On success, exactly one slice will be delivered on |resCh| for every message
// delivered in |reqCh|, and they will be delivered in order.
//
// This function handles backoff and retries for the underlying streaming RPC.
func fetcherRPCDownloadLocsThread(ctx context.Context, reqCh chan *remotesapi.GetDownloadLocsRequest, resCh chan []*remotesapi.DownloadLoc, client remotesapi.ChunkStoreServiceClient, storeRepoToken func(string), missingChunkCh chan nbs.ToChunker, host string) error {
	stream, err := reliable.MakeCall[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse](
		ctx,
		reliable.CallOptions[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse]{
			Open: func(ctx context.Context, opts ...grpc.CallOption) (reliable.ClientStream[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse], error) {
				return client.StreamDownloadLocations(ctx, opts...)
			},
			ErrF:               processGrpcErr,
			BackOffF:           grpcBackOff,
			ReadRequestTimeout: reliableCallReadRequestTimeout,
			DeliverRespTimeout: reliableCallDeliverRespTimeout,
		},
	)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for {
			select {
			case req, ok := <-reqCh:
				if !ok {
					return stream.CloseSend()
				}
				err := stream.Send(req)
				if err != nil {
					return NewRpcError(err, "StreamDownloadLocations", host, req)
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	eg.Go(func() error {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(resCh)
				return nil
			}
			if err != nil {
				return NewRpcError(err, "StreamDownloadLocations", host, stream.AssociatedReq())
			}
			if resp == nil {
				return NewRpcError(errors.New("no stream response"), "StreamDownloadLocations", host, stream.AssociatedReq())
			}
			if resp.RepoToken != "" {
				storeRepoToken(resp.RepoToken)
			}

			// Compute this before we pass resp.Locs along, since the next thread will own resp.Locs after we send it.
			if missingChunkCh != nil {
				req := stream.AssociatedReq()
				missing, err := getMissingChunks(req, resp)
				if err != nil {
					return err
				}
				for h := range missing {
					select {
					case missingChunkCh <- nbs.CompressedChunk{H: h}:
					case <-ctx.Done():
						return context.Cause(ctx)
					}
				}
			}

			select {
			case resCh <- resp.Locs:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	return eg.Wait()
}

// Peer of fetcherRPCDownloadLocsThread for the new StreamChunkLocations
// RPC. Reads request messages off |reqCh| and drives the streaming RPC,
// translating the deduplicated response shape back into
// []*remotesapi.DownloadLoc so the downstream coalescing thread is
// reused verbatim.
//
// Maintains |tfByID|, a long-lived table_file_id -> TableFileRecord map
// that is never reset. reliable.MakeCall may tear the underlying stream
// down and reopen it behind our back; a fresh handler restarts id
// assignment and re-introduces any id it reuses with a fresh
// TableFileRecord, which we overwrite into |tfByID| before resolving
// any ChunkLocation that references it. See the proto file.
func fetcherRPCChunkLocationsThread(ctx context.Context, reqCh chan *remotesapi.StreamChunkLocationsRequest, resCh chan []*remotesapi.DownloadLoc, client remotesapi.ChunkStoreServiceClient, storeRepoToken func(string), missingChunkCh chan nbs.ToChunker, host string, getRepoId func() (*remotesapi.RepoId, string), repoPath string) error {
	stream, err := reliable.MakeCall(
		ctx,
		reliable.CallOptions[*remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse]{
			Open: func(ctx context.Context, opts ...grpc.CallOption) (reliable.ClientStream[*remotesapi.StreamChunkLocationsRequest, *remotesapi.StreamChunkLocationsResponse], error) {
				return client.StreamChunkLocations(ctx, opts...)
			},
			ErrF:               processGrpcErr,
			BackOffF:           grpcBackOff,
			ReadRequestTimeout: reliableCallReadRequestTimeout,
			DeliverRespTimeout: reliableCallDeliverRespTimeout,
		},
	)
	if err != nil {
		return err
	}

	tfByID := make(map[uint32]*tableFileRec)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for {
			select {
			case req, ok := <-reqCh:
				if !ok {
					return stream.CloseSend()
				}
				if err := stream.Send(req); err != nil {
					return NewRpcError(err, "StreamChunkLocations", host, req)
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	eg.Go(func() error {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(resCh)
				return nil
			}
			req := stream.AssociatedReq()

			if err != nil {
				return NewRpcError(err, "StreamChunkLocations", host, req)
			}
			if resp == nil {
				return NewRpcError(errors.New("no stream response"), "StreamChunkLocations", host, req)
			}
			if resp.RepoToken != "" {
				storeRepoToken(resp.RepoToken)
			}

			// Integrate table-file records before resolving
			// locations: a ChunkLocation in this response may
			// reference an id introduced by a TableFileRecord in
			// the same response.
			for _, rec := range resp.TableFiles {
				tfByID[rec.TableFileId] = rec
			}

			if missingChunkCh != nil {
				for _, idx := range resp.MissingIndexes {
					bs, ok := hashAtIndex(req.ChunkHashes, idx)
					if !ok {
						return NewRpcError(errors.New("server returned missing_index out of range"), "StreamChunkLocations", host, req)
					}
					var h hash.Hash
					copy(h[:], bs)
					select {
					case missingChunkCh <- nbs.CompressedChunk{H: h}:
					case <-ctx.Done():
						return context.Cause(ctx)
					}
				}
			}

			locs, err := translateChunkLocations(req, resp.Locations, tfByID, getRepoId, repoPath)
			if err != nil {
				return NewRpcError(err, "StreamChunkLocations", host, req)
			}

			select {
			case resCh <- locs:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	return eg.Wait()
}

// Group the ChunkLocations in a response by table_file_id and emit one
// *remotesapi.DownloadLoc per group, so the downstream
// fetcherDownloadRangesThread pipeline is reused without modification.
// RefreshRequest is synthesized locally from the TableFileRecord's
// file_id plus the DoltChunkStore's current (repo_id, repo_token,
// repo_path) — the server no longer sends a per-DownloadLoc
// RefreshTableFileUrlRequest.
func translateChunkLocations(req *remotesapi.StreamChunkLocationsRequest, locations []*chunkLoc, tfByID map[uint32]*tableFileRec, getRepoId func() (*remotesapi.RepoId, string), repoPath string) ([]*remotesapi.DownloadLoc, error) {
	if len(locations) == 0 {
		return nil, nil
	}

	// Group by table_file_id, preserving first-seen order so the
	// output is deterministic.
	idOrder := make([]uint32, 0, len(tfByID))
	rangesByID := make(map[uint32][]*remotesapi.RangeChunk)
	for _, cl := range locations {
		bs, ok := hashAtIndex(req.ChunkHashes, cl.RequestIndex)
		if !ok {
			return nil, errors.New("server returned ChunkLocation.request_index out of range")
		}
		if _, ok := tfByID[cl.TableFileId]; !ok {
			return nil, fmt.Errorf("server referenced unknown table_file_id %d", cl.TableFileId)
		}
		if _, seen := rangesByID[cl.TableFileId]; !seen {
			idOrder = append(idOrder, cl.TableFileId)
		}
		rangesByID[cl.TableFileId] = append(rangesByID[cl.TableFileId], &remotesapi.RangeChunk{
			Hash:             bs,
			Offset:           cl.Offset,
			Length:           cl.Length,
			DictionaryOffset: cl.DictionaryOffset,
			DictionaryLength: cl.DictionaryLength,
		})
	}

	repoId, repoToken := getRepoId()
	out := make([]*remotesapi.DownloadLoc, 0, len(idOrder))
	for _, id := range idOrder {
		rec := tfByID[id]
		out = append(out, &remotesapi.DownloadLoc{
			Location: &remotesapi.DownloadLoc_HttpGetRange{
				HttpGetRange: &remotesapi.HttpGetRange{
					Url:    rec.Url,
					Ranges: rangesByID[id],
				},
			},
			RefreshAfter: rec.RefreshAfter,
			RefreshRequest: &remotesapi.RefreshTableFileUrlRequest{
				RepoId:    repoId,
				RepoToken: repoToken,
				RepoPath:  repoPath,
				FileId:    rec.FileId,
			},
		})
	}
	return out, nil
}

func getMissingChunks(req *remotesapi.GetDownloadLocsRequest, resp *remotesapi.GetDownloadLocsResponse) (hash.HashSet, error) {
	numRequested := len(req.ChunkHashes)
	numResponded := 0
	for _, loc := range resp.Locs {
		hgr := loc.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		numResponded += len(hgr.Ranges)
	}
	if numResponded > numRequested {
		return nil, errors.New("possible internal error: server responded with more chunks than we asked for in StreamDownloadLocations")
	}
	if numResponded == numRequested {
		// XXX: We assume it's the same chunks and that the server is well behaved.
		return nil, nil
	}
	requested := make(hash.HashSet, numRequested)
	for _, ch := range req.ChunkHashes {
		var h hash.Hash
		copy(h[:], ch)
		requested.Insert(h)
	}
	for _, loc := range resp.Locs {
		hgr := loc.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
		for _, rc := range hgr.Ranges {
			var h hash.Hash
			copy(h[:], rc.Hash)
			requested.Remove(h)
		}
	}
	return requested, nil
}

// Delivered to a fetching thread by the download-coalescing thread, requests for the
// fetching thread to download all the entries in the |GetRange| and deliver them to
// the appropriate places. For |rangeType| Chunk, it delivers them to the fetched
// chunk channel. For |rangeType| Dictionary, it delivers them by calling |set| on the
// dictionaryCache with the fetched dictionary.
type fetchResp struct {
	get       *GetRange
	refresh   func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error)
	rangeType rangeType
	dictCache *dictionaryCache
	path      string
	// multiRange indicates the worker should use
	// GetMultiRangeDownloadFunc (comma-separated Range header with
	// multipart/byteranges response) instead of the single-range
	// GetDownloadFunc.
	multiRange bool
}

type fetchReq struct {
	respCh chan fetchResp
}

// A simple structure to keep track of *GetRange requests along with
// |locationRefreshes| for the URL paths we have seen.
//
// When |multiRangeEnabled| is false (default), |chunkRanges| and
// |dictRanges| are the authoritative stores; the MRQueue fields are
// nil. When |multiRangeEnabled| is true, the roles are reversed: the
// Tree fields are nil and the MRQueue fields hold pending chunks for
// multi-range HTTP dispatch.
//
// Dictionary ranges and chunk ranges are kept in separate stores in
// both modes: chunk worker threads have a blocking data dependency on
// dictionary bytes being resolved, and keeping dictionary fetches on
// their own path means chunk workers never have to wait on an
// out-of-order dict response bundled into the same request.
type downloads struct {
	// Tree path (multi-range disabled).
	chunkRanges *ranges.Tree
	dictRanges  *ranges.Tree
	// MRQueue path (DOLT_MULTI_RANGE=1).
	chunkMR *ranges.MRQueue
	dictMR  *ranges.MRQueue

	// Holds all pending and fetched dictionaries for any chunk
	// ranges that have gone into |chunkRanges|.
	dictCache *dictionaryCache
	refreshes map[string]*locationRefresh
}

func newDownloads() downloads {
	d := downloads{
		dictCache: &dictionaryCache{},
		refreshes: make(map[string]*locationRefresh),
	}
	if multiRangeEnabled {
		d.chunkMR = ranges.NewMRQueue()
		d.dictMR = ranges.NewMRQueue()
	} else {
		d.chunkRanges = ranges.NewTree(chunkAggDistance)
		d.dictRanges = ranges.NewTree(chunkAggDistance)
	}
	return d
}

func (d downloads) chunkLen() int {
	if multiRangeEnabled {
		return d.chunkMR.Len()
	}
	return d.chunkRanges.Len()
}

func (d downloads) dictLen() int {
	if multiRangeEnabled {
		return d.dictMR.Len()
	}
	return d.dictRanges.Len()
}

func (d downloads) Add(resp *remotesapi.DownloadLoc) {
	hgr := resp.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
	path := ResourcePath(hgr.Url)
	if v, ok := d.refreshes[path]; ok {
		v.Add(resp)
	} else {
		refresh := new(locationRefresh)
		refresh.Add(resp)
		d.refreshes[path] = refresh
	}
	for _, r := range hgr.Ranges {
		if r.DictionaryLength != 0 {
			_, has := d.dictCache.getOrCreate(path, r.DictionaryOffset, r.DictionaryLength)
			if !has {
				if !multiRangeEnabled && netstats.Enabled() {
					netstats.Global().CheckDarkCacheHit(hgr.Url, r.DictionaryOffset, uint64(r.DictionaryLength))
				}
				if multiRangeEnabled {
					d.dictMR.Insert(hgr.Url, nil, r.DictionaryOffset, r.DictionaryLength, 0, 0)
				} else {
					d.dictRanges.Insert(hgr.Url, nil, r.DictionaryOffset, r.DictionaryLength, 0, 0)
				}
			}
		}
		if !multiRangeEnabled && netstats.Enabled() {
			netstats.Global().CheckDarkCacheHit(hgr.Url, r.Offset, uint64(r.Length))
		}
		if multiRangeEnabled {
			d.chunkMR.Insert(hgr.Url, r.Hash[:], r.Offset, r.Length, r.DictionaryOffset, r.DictionaryLength)
		} else {
			d.chunkRanges.Insert(hgr.Url, r.Hash[:], r.Offset, r.Length, r.DictionaryOffset, r.DictionaryLength)
		}
	}
}

func toGetRange(rs []*ranges.GetRange) *GetRange {
	ret := new(GetRange)
	for _, r := range rs {
		ret.Url = r.Url
		ret.Ranges = append(ret.Ranges, &Range{
			Hash:       r.Hash,
			Offset:     r.Offset,
			Length:     r.Length,
			DictOffset: r.DictOffset,
			DictLength: r.DictLength,
		})
	}
	return ret
}

type rangeType int

const (
	rangeType_Chunk rangeType = iota
	rangeType_Dictionary
)

// Reads off |locCh| and assembles DownloadLocs into download ranges.
func fetcherDownloadRangesThread(ctx context.Context, locCh chan []*remotesapi.DownloadLoc, fetchReqCh chan fetchReq, doneCh chan struct{}) error {
	downloads := newDownloads()
	for {
		hasWork := downloads.dictLen() > 0 || downloads.chunkLen() > 0
		if !hasWork && locCh == nil {
			// Once |locCh| closes, we sit it to |nil|. If
			// our range trees are empty then we have
			// already delivered every download we will
			// ever see to a download thread.
			close(doneCh)
			return nil
		}
		var reqCh chan fetchReq
		if hasWork {
			reqCh = fetchReqCh
		}
		select {
		case req, ok := <-locCh:
			if !ok {
				locCh = nil
			} else {
				for _, loc := range req {
					downloads.Add(loc)
				}
			}
		case req := <-reqCh:
			resp := dispatchOne(downloads)
			// Requester should deliver an exclusive,
			// buffered channel where this deliver always
			// succeeds.
			req.respCh <- resp
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// dispatchOne pops one unit of work from |downloads| — either the
// next Region from the Tree (legacy single-range path) or the next
// multi-range request's worth of chunks from the MRQueue. Dicts are
// dispatched before chunks in both modes.
func dispatchOne(downloads downloads) fetchResp {
	if multiRangeEnabled {
		var groups [][]*ranges.GetRange
		var url string
		var bridgedDark uint64
		var toSendType rangeType
		if downloads.dictLen() > 0 {
			groups, url, bridgedDark = downloads.dictMR.PopRequest(multiRangeMaxBytes, multiRangeMaxRanges, multiRangeSlop)
			toSendType = rangeType_Dictionary
		} else {
			groups, url, bridgedDark = downloads.chunkMR.PopRequest(multiRangeMaxBytes, multiRangeMaxRanges, multiRangeSlop)
			toSendType = rangeType_Chunk
		}
		if netstats.Enabled() && bridgedDark > 0 {
			netstats.Global().RecordMultiRangeBridgedDark(bridgedDark)
		}
		var flat []*ranges.GetRange
		for _, g := range groups {
			flat = append(flat, g...)
		}
		toSend := toGetRange(flat)
		if toSend.Url == "" {
			// toGetRange only sets Url from the first entry;
			// when flat is empty we must still send the URL
			// we popped so the refresh lookup works.
			toSend.Url = url
		}
		return buildFetchResp(downloads, toSend, toSendType, true)
	}

	var toSendType rangeType
	var dark uint64
	var region *ranges.Region
	var max []*ranges.GetRange
	if downloads.dictRanges.Len() > 0 {
		max, region, dark = downloads.dictRanges.DeleteMaxRegion()
		toSendType = rangeType_Dictionary
	} else {
		max, region, dark = downloads.chunkRanges.DeleteMaxRegion()
		toSendType = rangeType_Chunk
	}
	if netstats.Enabled() {
		if dark > 0 {
			netstats.Global().RecordDispatchedDarkBytes(dark)
		}
		if region != nil {
			netstats.Global().RecordDispatchedRegion(region.Url, region.StartOffset, region.EndOffset)
		}
	}
	return buildFetchResp(downloads, toGetRange(max), toSendType, false)
}

func buildFetchResp(downloads downloads, toSend *GetRange, t rangeType, multiRange bool) fetchResp {
	path := toSend.ResourcePath()
	refresh := downloads.refreshes[path]
	return fetchResp{
		get: toSend,
		refresh: func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error) {
			return refresh.GetURL(ctx, err, client)
		},
		rangeType:  t,
		path:       path,
		dictCache:  downloads.dictCache,
		multiRange: multiRange,
	}
}

type ConcurrencyControl struct {
	MaxConcurrency int

	failures  atomic.Int64
	successes atomic.Int64
}

func (cc *ConcurrencyControl) RecordSuccess() {
	cc.successes.Add(1)
}

func (cc *ConcurrencyControl) RecordFailure() {
	cc.failures.Add(1)
}

type SizeSetter interface {
	SetSize(int)
}

// This does additive increase, multiplicative decrease on calls to |SetSize|,
// reading successes and failures from calls to |RecordSuccess| and
// |RecordFailure|. If there have been any failures in the last update window,
// it will call |SetSize| with a new size that's 1/2 the current size. If there
// have been no failures in the last update window, but there has been at least
// one success, it will call |SetSize| with a size 1 greater than the current
// size. Will not scale size greater than |MaxConcurrency|.
func (cc *ConcurrencyControl) Run(ctx context.Context, done <-chan struct{}, ss SizeSetter, sz int) error {
	var justDecreased bool
	const (
		defaultConcurrencyAdjustmentDuration = 500 * time.Millisecond
		backoffConcurrentAdjustmentDuration  = 5 * time.Second
	)
	next := defaultConcurrencyAdjustmentDuration
	var lastS int64
	for {
		select {
		case <-time.After(next):
			f := cc.failures.Load()
			if f > 0 && !justDecreased {
				sz = (sz + 1) / 2
				ss.SetSize(sz)
				justDecreased = true
				next = backoffConcurrentAdjustmentDuration
			} else {
				next = defaultConcurrencyAdjustmentDuration
				s := cc.successes.Load()
				if f == 0 && s > lastS && sz < cc.MaxConcurrency {
					sz += 1
					ss.SetSize(sz)
					lastS = s
				}
				cc.failures.Store(0)
				justDecreased = false
			}
		case <-done:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func fetcherDownloadURLThreads(ctx context.Context, fetchReqCh chan fetchReq, doneCh chan struct{}, chunkCh chan nbs.ToChunker, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, fetcher HTTPFetcher, params NetworkRequestParams) error {
	eg, ctx := errgroup.WithContext(ctx)
	cc := &ConcurrencyControl{
		MaxConcurrency: params.MaximumConcurrentDownloads,
	}
	f := func(ctx context.Context, shutdownCh <-chan struct{}) error {
		return fetcherDownloadURLThread(ctx, fetchReqCh, shutdownCh, chunkCh, client, stats, cc, fetcher, params)
	}
	threads := pool.NewDynamic(ctx, f, params.StartingConcurrentDownloads)
	eg.Go(func() error {
		return threads.Run()
	})
	eg.Go(func() error {
		select {
		case <-doneCh:
			threads.Close()
		case <-ctx.Done():
			threads.Close()
		}
		return nil
	})
	eg.Go(func() error {
		return cc.Run(ctx, doneCh, threads, params.StartingConcurrentDownloads)
	})
	err := eg.Wait()
	if err != nil {
		return err
	}
	close(chunkCh)
	return nil
}

func deliverChunkCallback(chunkCh chan nbs.ToChunker, path string, dictCache *dictionaryCache) func(context.Context, []byte, *Range) error {
	return func(ctx context.Context, bs []byte, rang *Range) error {
		h := hash.New(rang.Hash[:])
		var cc nbs.ToChunker
		if rang.DictLength != 0 {
			payload, _ := dictCache.getOrCreate(path, rang.DictOffset, rang.DictLength)
			bundle, err := payload.Get()
			if err != nil {
				return err
			}

			cc = nbs.NewArchiveToChunker(h, bundle, bs)
		} else {
			var err error
			cc, err = nbs.NewCompressedChunk(h, bs)
			if err != nil {
				return err
			}
		}
		select {
		case chunkCh <- cc:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		return nil
	}
}

func setDictionaryCallback(dictCache *dictionaryCache, path string) func(context.Context, []byte, *Range) error {
	return func(ctx context.Context, bs []byte, rang *Range) error {
		bundle, err := nbs.NewDecompBundle(bs)
		if err != nil {
			return err
		}

		payload, _ := dictCache.getOrCreate(path, rang.Offset, rang.Length)
		payload.Set(bundle, err)
		// XXX: For now, we fail here on any error, instead of when we try to use the dictionary...
		// For now, the record in the cache will be terminally failed and is never removed.
		return err
	}
}

func fetcherDownloadURLThread(ctx context.Context, fetchReqCh chan fetchReq, doneCh <-chan struct{}, chunkCh chan nbs.ToChunker, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, health reliable.HealthRecorder, fetcher HTTPFetcher, params NetworkRequestParams) error {
	respCh := make(chan fetchResp, 1)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-doneCh:
			return nil
		case fetchReqCh <- fetchReq{respCh: respCh}:
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case fetchResp := <-respCh:
				var cb func(context.Context, []byte, *Range) error
				if fetchResp.rangeType == rangeType_Chunk {
					cb = deliverChunkCallback(chunkCh, fetchResp.path, fetchResp.dictCache)
				} else {
					cb = setDictionaryCallback(fetchResp.dictCache, fetchResp.path)
				}
				urlF := func(ctx context.Context, lastError error, resourcePath string) (string, error) {
					return fetchResp.refresh(ctx, lastError, client)
				}
				var f func() error
				if fetchResp.multiRange {
					f = fetchResp.get.GetMultiRangeDownloadFunc(ctx, stats, health, fetcher, params, cb, urlF)
				} else {
					f = fetchResp.get.GetDownloadFunc(ctx, stats, health, fetcher, params, cb, urlF)
				}
				err := f()
				if err != nil {
					return err
				}
			}
		}
	}
}

// A dictionaryCache provides a rendezvous point for chunk fetches
// which have data dependencies on dictionary fetches. It stores a
// single record per |path|,|offset| tuple we see, and that record
// will be populated with the |*nbs.DecompBundle| that results from
// fetching those contents. Every |GetRange| that has a dictionary
// dependency gets the record out of the dictionary cache. The first
// time the cache entry is created, the download thread also schedules
// the dictionary itself to be fetched and populated through |Set|.
type dictionaryCache struct {
	dictionaries sync.Map
}

// DictionaryKey is the a globaly unique identifier for an archive dictionary.
type DictionaryKey struct {
	// This is the short url to the resource, not including the query parameters - which are provided by the
	// locationRefresher.
	path string
	off  uint64
	len  uint32
}

type DictionaryPayload struct {
	done chan struct{}
	res  *nbs.DecompBundle
	err  error
}

func (p *DictionaryPayload) Get() (*nbs.DecompBundle, error) {
	<-p.done
	return p.res, p.err
}

func (p *DictionaryPayload) Set(res *nbs.DecompBundle, err error) {
	p.res = res
	p.err = err
	close(p.done)
}

func (dc *dictionaryCache) getOrCreate(path string, offset uint64, length uint32) (*DictionaryPayload, bool) {
	key := DictionaryKey{path, offset, length}
	entry, loaded := dc.dictionaries.LoadOrStore(key, &DictionaryPayload{done: make(chan struct{})})
	payload := entry.(*DictionaryPayload)
	return payload, loaded
}
