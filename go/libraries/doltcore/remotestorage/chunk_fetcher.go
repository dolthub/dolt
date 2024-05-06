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
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/pool"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/ranges"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/reliable"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// A remotestorage.ChunkFetcher is a pipelined chunk fetcher for fetching a
// large number of chunks where the downloads may benefit from range
// coallescing, hedging, automatic retries, pipelining of download location
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

	toGetCh chan hash.HashSet
	resCh   chan nbs.CompressedChunk

	abortCh chan struct{}
	stats   StatsRecorder
}

const (
	getLocsBatchSize = 512
)

func NewChunkFetcher(ctx context.Context, dcs *DoltChunkStore) *ChunkFetcher {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &ChunkFetcher{
		eg:    eg,
		egCtx: ctx,

		toGetCh: make(chan hash.HashSet),
		resCh:   make(chan nbs.CompressedChunk),

		abortCh: make(chan struct{}),
		stats:   StatsFactory(),
	}

	locsReqCh := make(chan *remotesapi.GetDownloadLocsRequest)
	downloadLocCh := make(chan []*remotesapi.DownloadLoc)
	locDoneCh := make(chan struct{})
	fetchReqCh := make(chan fetchReq)

	eg.Go(func() error {
		return fetcherHashSetToGetDlLocsReqsThread(ctx, ret.toGetCh, ret.abortCh, locsReqCh, getLocsBatchSize, dcs.repoPath, dcs.getRepoId)
	})
	eg.Go(func() error {
		return fetcherRPCDownloadLocsThread(ctx, locsReqCh, downloadLocCh, dcs.csClient, func(s string) { dcs.repoToken.Store(s) }, ret.resCh, dcs.host)
	})
	eg.Go(func() error {
		return fetcherDownloadRangesThread(ctx, downloadLocCh, fetchReqCh, locDoneCh)
	})
	eg.Go(func() error {
		return fetcherDownloadURLThreads(ctx, fetchReqCh, locDoneCh, ret.resCh, dcs.csClient, ret.stats, dcs.httpFetcher, dcs.params)
	})

	return ret
}

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

func (f *ChunkFetcher) CloseSend() error {
	close(f.toGetCh)
	return nil
}

func (f *ChunkFetcher) Recv(ctx context.Context) (nbs.CompressedChunk, error) {
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

func (f *ChunkFetcher) Close() error {
	defer StatsFlusher(f.stats)
	close(f.abortCh)
	return f.eg.Wait()
}

// Reads HashSets from reqCh and batches all the received addresses
// into |GetDownloadLocsRequest| messages with up to |batchSize| chunk hashes
// in them. It delivers the messages to be send on |resCh|.
func fetcherHashSetToGetDlLocsReqsThread(ctx context.Context, reqCh chan hash.HashSet, abortCh chan struct{}, resCh chan *remotesapi.GetDownloadLocsRequest, batchSize int, repoPath string, idFunc func() (*remotesapi.RepoId, string)) error {
	var addrs [][]byte
	var outbound [][]byte
	for {
		if reqCh == nil && len(addrs) == 0 {
			close(resCh)
			return nil
		}

		var thisResCh chan *remotesapi.GetDownloadLocsRequest
		var thisRes *remotesapi.GetDownloadLocsRequest

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
			id, token := idFunc()
			thisRes = &remotesapi.GetDownloadLocsRequest{RepoId: id, RepoPath: repoPath, RepoToken: token, ChunkHashes: outbound[:]}
			thisResCh = resCh
		}

		select {
		case hs, ok := <-reqCh:
			if !ok {
				reqCh = nil
				break
			}
			for h := range hs {
				h := h
				addrs = append(addrs, h[:])
			}
		case thisResCh <- thisRes:
			addrs = addrs[:len(addrs)-len(thisRes.ChunkHashes)]
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
func fetcherRPCDownloadLocsThread(ctx context.Context, reqCh chan *remotesapi.GetDownloadLocsRequest, resCh chan []*remotesapi.DownloadLoc, client remotesapi.ChunkStoreServiceClient, storeRepoToken func(string), missingChunkCh chan nbs.CompressedChunk, host string) error {
	stream, err := reliable.MakeCall[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse](
		ctx,
		reliable.CallOptions[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse]{
			Open: func(ctx context.Context, opts ...grpc.CallOption) (reliable.ClientStream[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse], error) {
				return client.StreamDownloadLocations(ctx, opts...)
			},
			ErrF:               processGrpcErr,
			BackOffF:           grpcBackOff,
			ReadRequestTimeout: 15 * time.Second,
			DeliverRespTimeout: 15 * time.Second,
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
				// TODO: There is no timeout or deadline here.
				// We could impose one with a cancelable
				// context and some ticking here, or we could
				// impose one in the sending thread,
				// fetcherHashSetToGetDlLocsReqsThread, which
				// could timeout if it can't deliver a download
				// locs request here for a long time...
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

type fetchResp struct {
	get     *GetRange
	refresh func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error)
}

type fetchReq struct {
	respCh   chan fetchResp
	cancelCh chan struct{}
}

type downloads struct {
	ranges    *ranges.Tree
	refreshes map[string]*locationRefresh
}

func newDownloads() downloads {
	return downloads{
		ranges:    ranges.NewTree(chunkAggDistance),
		refreshes: make(map[string]*locationRefresh),
	}
}

func (d downloads) Add(resp *remotesapi.DownloadLoc) {
	gr := (*GetRange)(resp.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange)
	path := gr.ResourcePath()
	if v, ok := d.refreshes[path]; ok {
		v.Add(resp)
	} else {
		refresh := new(locationRefresh)
		refresh.Add(resp)
		d.refreshes[path] = refresh
	}
	for _, r := range gr.Ranges {
		d.ranges.Insert(gr.Url, r.Hash, r.Offset, r.Length)
	}
}

func toGetRange(rs []*ranges.GetRange) *GetRange {
	ret := new(GetRange)
	for _, r := range rs {
		ret.Url = r.Url
		ret.Ranges = append(ret.Ranges, &remotesapi.RangeChunk{
			Hash:   r.Hash,
			Offset: r.Offset,
			Length: r.Length,
		})
	}
	return ret
}

// Reads off |locCh| and assembles DownloadLocs into download ranges.
func fetcherDownloadRangesThread(ctx context.Context, locCh chan []*remotesapi.DownloadLoc, fetchReqCh chan fetchReq, doneCh chan struct{}) error {
	downloads := newDownloads()
	pending := make([]fetchReq, 0)
	var toSend *GetRange
	for {
		for j := range pending {
			if toSend == nil {
				max := downloads.ranges.DeleteMaxRegion()
				if len(max) == 0 {
					break
				}
				toSend = toGetRange(max)
			}
			path := toSend.ResourcePath()
			refresh := downloads.refreshes[path]

			resp := fetchResp{
				get: toSend,
				refresh: func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error) {
					return refresh.GetURL(ctx, err, client)
				},
			}

			select {
			case pending[j].respCh <- resp:
				toSend = nil
			case <-pending[j].cancelCh:
			case <-ctx.Done():
				return context.Cause(ctx)
			}

			pending[j].respCh = nil
		}

		newpending := make([]fetchReq, 0)
		for i := range pending {
			if pending[i].respCh != nil {
				newpending = append(newpending, pending[i])
			}
		}
		pending = newpending

		if locCh == nil && downloads.ranges.Len() == 0 {
			close(doneCh)
			return nil
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
		case req := <-fetchReqCh:
			pending = append(pending, req)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
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

func (cc *ConcurrencyControl) Run(ctx context.Context, done <-chan struct{}, ss SizeSetter, sz int) error {
	var justDecreased bool
	next := 500 * time.Millisecond
	var lastS int64
	for {
		select {
		case <-time.After(next):
			f := cc.failures.Load()
			if f > 0 && !justDecreased {
				sz = (sz + 1) / 2
				ss.SetSize(sz)
				justDecreased = true
				next = 5 * time.Second
			} else {
				next = 500 * time.Millisecond
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

func fetcherDownloadURLThreads(ctx context.Context, fetchReqCh chan fetchReq, doneCh chan struct{}, chunkCh chan nbs.CompressedChunk, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, fetcher HTTPFetcher, params NetworkRequestParams) error {
	eg, ctx := errgroup.WithContext(ctx)
	cc := &ConcurrencyControl{
		MaxConcurrency: params.MaximumConcurrentDownloads,
	}
	f := func(ctx context.Context, shutdownCh <-chan struct{}) error {
		return fetcherDownloadURLThread(ctx, fetchReqCh, shutdownCh, chunkCh, client, stats, cc, fetcher, params)
	}
	threads := pool.NewDynamic(f, params.StartingConcurrentDownloads)
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

func fetcherDownloadURLThread(ctx context.Context, fetchReqCh chan fetchReq, doneCh <-chan struct{}, chunkCh chan nbs.CompressedChunk, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, health reliable.HealthRecorder, fetcher HTTPFetcher, params NetworkRequestParams) error {
	respCh := make(chan fetchResp)
	cancelCh := make(chan struct{})
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-doneCh:
			return nil
		case fetchReqCh <- fetchReq{respCh: respCh, cancelCh: cancelCh}:
			select {
			case <-doneCh:
				close(cancelCh)
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			case fetchResp := <-respCh:
				f := fetchResp.get.GetDownloadFunc(ctx, stats, health, fetcher, params, chunkCh, func(ctx context.Context, lastError error, resourcePath string) (string, error) {
					return fetchResp.refresh(ctx, lastError, client)
				})
				err := f()
				if err != nil {
					return err
				}
			}
		}
	}
}
