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
	"math"
	"time"

	"fmt"
	"github.com/fatih/color"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
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
		return fetcherRPCDownloadLocsThread(ctx, locsReqCh, downloadLocCh, dcs.csClient)
	})
	eg.Go(func() error {
		return fetcherDownloadRangesThread(ctx, downloadLocCh, fetchReqCh, locDoneCh)
	})
	eg.Go(func() error {
		return fetcherDownloadURLThreads(ctx, fetchReqCh, locDoneCh, ret.resCh, dcs.csClient, ret.stats, dcs.httpFetcher, uint64(dcs.concurrency.LargeFetchSize), dcs.concurrency.ConcurrentSmallFetches, dcs.concurrency.ConcurrentLargeFetches)
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
func fetcherRPCDownloadLocsThread(ctx context.Context, reqCh chan *remotesapi.GetDownloadLocsRequest, resCh chan []*remotesapi.DownloadLoc, client remotesapi.ChunkStoreServiceClient) error {
	stream, err := reliable.MakeCall[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse](
		ctx,
		reliable.CallOptions[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse]{
			Open: func(ctx context.Context, opts ...grpc.CallOption) (reliable.ClientStream[*remotesapi.GetDownloadLocsRequest, *remotesapi.GetDownloadLocsResponse], error) {
				return client.StreamDownloadLocations(ctx, opts...)
			},
			ErrF: processGrpcErr,
			BackOffF: grpcBackOff,
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
				err := stream.Send(req)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	// TODO: Need to structure the Recv() here, and resCh, or something, so that missing chunks can be accounted for here.
	// Because the req/resp are 1-to-1, every missing chunk will be evident in the resp for the corresponding req.
	eg.Go(func() error {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(resCh)
				return nil
			}
			if err != nil {
				return err
			}
			// TODO: repoToken.Store()
			select {
			case resCh <- resp.Locs:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	return eg.Wait()
}

type fetchResp struct {
	get     *GetRange
	refresh func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error)
}

type fetchReq struct {
	respCh chan fetchResp
	minSz  uint64
	maxSz  uint64
}

// Reads off |locCh| and assembles DownloadLocs into download ranges.
func fetcherDownloadRangesThread(ctx context.Context, locCh chan []*remotesapi.DownloadLoc, fetchReqCh chan fetchReq, doneCh chan struct{}) error {
	locs := newDlLocations()
	pending := make([]fetchReq, 0)
	for {
		numGaps := 0
		numSends := 0
		lenSends := 0
		sent := true
		for sent {
			sent = false
			for j := range pending {
				for path, gr := range locs.ranges {
					split := gr.SplitAtGaps(chunkAggDistance)
					numGaps += len(split)
					var i int
					for i = 0; i < len(split); i++ {
						l := split[i].RangeLen()
						if l >= pending[j].minSz && l < pending[j].maxSz {
							refresh := locs.refreshes[path]
							select {
							case pending[j].respCh <- fetchResp{
								get: split[i],
								refresh: func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error) {
									return refresh.GetURL(ctx, err, client)
								},
							}:
							case <-ctx.Done():
								return context.Cause(ctx)
							}
							pending[j].respCh = nil
							sent = true
							numSends += 1
							lenSends += int(l)
							break
						}
					}
					if i != len(split) {
						newranges := make([]*remotesapi.RangeChunk, 0, len(gr.Ranges)-len(split[i].Ranges))
						for j := 0; j < len(split); j++ {
							if j == i {
								continue
							}
							newranges = append(newranges, split[j].Ranges...)
						}
						gr.Ranges = newranges
						break
					}
				}
			}
			newpending := make([]fetchReq, 0)
			for i := range pending {
				if pending[i].respCh != nil {
					newpending = append(newpending, pending[i])
				}
			}
			pending = newpending
		}

		if numSends > 0 || len(pending) > 0 {
			numRanges := 0
			for _, gr := range locs.ranges {
				numRanges += len(gr.Ranges)
			}
	
			fmt.Fprintf(color.Error, "%v reliable.grpc blockForDeliverResp; len(pending): %v, len(locs): %v, numGaps: %v, numSends: %d, lenSends: %d\n", time.Now(), len(pending), numRanges, numGaps, numSends, lenSends)
		}

		select {
		case req, ok := <-locCh:
			if !ok {
				close(doneCh)
				return nil
			}
			for _, loc := range req {
				locs.Add(loc)
			}
		case req := <-fetchReqCh:
			pending = append(pending, req)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func fetcherDownloadURLThreads(ctx context.Context, fetchReqCh chan fetchReq, doneCh chan struct{}, chunkCh chan nbs.CompressedChunk, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, fetcher HTTPFetcher, largeFetchSz uint64, smallFetches, largeFetches int) error {
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < smallFetches; i++ {
		eg.Go(func() error {
			return fetcherDownloadURLThread(ctx, fetchReqCh, doneCh, chunkCh, 0, math.MaxUint64, client, stats, fetcher)
		})
	}
//	for i := 0; i < largeFetches; i++ {
//		eg.Go(func() error {
//			return fetcherDownloadURLThread(ctx, fetchReqCh, doneCh, chunkCh, largeFetchSz, math.MaxUint64, client, stats, fetcher)
//		})
//	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	close(chunkCh)
	return nil
}

func fetcherDownloadURLThread(ctx context.Context, fetchReqCh chan fetchReq, doneCh chan struct{}, chunkCh chan nbs.CompressedChunk, minSz, maxSz uint64, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, fetcher HTTPFetcher) error {
	respCh := make(chan fetchResp)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-doneCh:
			return nil
		case fetchReqCh <- fetchReq{respCh: respCh, minSz: minSz, maxSz: maxSz}:
			select {
			case <-doneCh:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			case fetchResp := <-respCh:
				f := fetchResp.get.GetDownloadFunc(ctx, stats, fetcher, chunkCh, func(ctx context.Context, lastError error, resourcePath string) (string, error) {
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
