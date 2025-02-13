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
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/gozstd"
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
// into |GetDownloadLocsRequest| messages with up to |batchSize| chunk hashes
// in them. It delivers the batched messages to |resCh|.
func fetcherHashSetToGetDlLocsReqsThread(ctx context.Context, reqCh chan hash.HashSet, abortCh chan struct{}, resCh chan *remotesapi.GetDownloadLocsRequest, batchSize int, repoPath string, idFunc func() (*remotesapi.RepoId, string)) error {
	// This is the buffer of received that we haven't sent to |resCh| yet.
	var addrs [][]byte
	// This is the current slice we're trying to send in a
	// |GetDownloadLocsRequest|.  After we send it successfully, we will
	// need to allocate a new one for the next message, but we can reuse
	// its memory when we fail to send on |resCh| to form the next download
	// request we try to send.
	var outbound [][]byte
	for {
		if reqCh == nil && len(addrs) == 0 {
			close(resCh)
			return nil
		}

		var thisResCh chan *remotesapi.GetDownloadLocsRequest
		var thisRes *remotesapi.GetDownloadLocsRequest

		// Each time through the loop, we build a new
		// |GetDownloadLocsRequest| to send. It contains up to
		// |batchSize| hashes from the end of |addrs|. If we
		// successfully send it, then we will drop those addresses from
		// the end of |addrs|.
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
}

type fetchReq struct {
	respCh   chan fetchResp
}

// A simple structure to keep track of *GetRange requests along with
// |locationRefreshes| for the URL paths we have seen.
type downloads struct {
	// This Tree exclusively holds Chunk fetch ranges.
	chunkRanges *ranges.Tree
	// This Tree exclusively holds Dictionary fetch ranges. Every
	// entry that we create in |dictionaryCache| which needs to be
	// populated goes in here. These ranges must be fetched before
	// (or concurrently with) any chunkRanges, since the chunk
	// range fetches will block the fetching thread on the
	// population of the dictionary cache entry.
	dictRanges  *ranges.Tree
	// Holds all pending and fetched dictionaries for any chunk
	// ranges that have gone into |chunkRanges|.
	dictCache   *dictionaryCache
	refreshes   map[string]*locationRefresh
}

func newDownloads() downloads {
	return downloads{
		chunkRanges: ranges.NewTree(chunkAggDistance),
		dictRanges:  ranges.NewTree(chunkAggDistance),
		dictCache:   &dictionaryCache{},
		refreshes:   make(map[string]*locationRefresh),
	}
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
		var getDict func() (any, error)
		if r.DictionaryLength != 0 {
			var first bool
			getDict, first = d.dictCache.get(path, r.DictionaryOffset, r.DictionaryLength)
			if first {
				d.dictRanges.Insert(hgr.Url, nil, r.DictionaryOffset, r.DictionaryLength, nil)
			}
		}
		d.chunkRanges.Insert(hgr.Url, r.Hash[:], r.Offset, r.Length, getDict)
	}
}

func toGetRange(rs []*ranges.GetRange) *GetRange {
	ret := new(GetRange)
	for _, r := range rs {
		ret.Url = r.Url
		ret.Ranges = append(ret.Ranges, &Range{
			Hash:    r.Hash,
			Offset:  r.Offset,
			Length:  r.Length,
			GetDict: r.GetDict,
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
		hasWork := downloads.dictRanges.Len() > 0 || downloads.chunkRanges.Len() > 0
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
			var toSend *GetRange
			var toSendType rangeType
			if downloads.dictRanges.Len() > 0 {
				max := downloads.dictRanges.DeleteMaxRegion()
				toSend, toSendType = toGetRange(max), rangeType_Dictionary
			} else {
				// Necessarily non-empty, since |hasWork| is true...
				max := downloads.chunkRanges.DeleteMaxRegion()
				toSend, toSendType = toGetRange(max), rangeType_Chunk
			}
			path := toSend.ResourcePath()
			refresh := downloads.refreshes[path]
			resp := fetchResp{
				get: toSend,
				refresh: func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error) {
					return refresh.GetURL(ctx, err, client)
				},
				rangeType: toSendType,
				path:      path,
				dictCache: downloads.dictCache,
			}
			// Requester should deliver an exclusive,
			// buffered channel where this deliver always
			// succeeds.
			req.respCh <- resp
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

func deliverChunkCallback(chunkCh chan nbs.ToChunker) func(context.Context, []byte, *Range) error {
	return func(ctx context.Context, bs []byte, rang *Range) error {
		h := hash.New(rang.Hash[:])
		var cc nbs.ToChunker
		if rang.GetDict != nil {
			dictRes, err := rang.GetDict()
			if err != nil {
				return err
			}
			cc = nbs.NewArchiveToChunker(h, dictRes.(*gozstd.DDict), bs)
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
		var ddict *gozstd.DDict
		decompressed, err := gozstd.Decompress(nil, bs)
		if err == nil {
			ddict, err = gozstd.NewDDict(decompressed)
		}
		dictCache.set(path, rang.Offset, rang.Length, ddict, err)
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
					cb = deliverChunkCallback(chunkCh)
				} else {
					cb = setDictionaryCallback(fetchResp.dictCache, fetchResp.path)
				}
				f := fetchResp.get.GetDownloadFunc(ctx, stats, health, fetcher, params, cb, func(ctx context.Context, lastError error, resourcePath string) (string, error) {
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

// A dictionaryCache provides a rendezvous point for chunk fetches
// which have data dependencies on dictionary fetches. It stores a
// single record per |path|,|offset| tuple we see, and that record
// will be populated with the |*gozstd.DDict| that results from
// fetching those contents. Every |GetRange| that has a dictionary
// dependency gets the record out of the dictionary cache. The first
// time the cache entry is created, the download thread also schedules
// the dictionary itself to be fetched and populated through |set|.
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
	res  any
	err  error
}

func (dc *dictionaryCache) get(path string, offset uint64, length uint32) (func() (any, error), bool) {
	key := DictionaryKey{path, offset, length}
	entry, loaded := dc.dictionaries.LoadOrStore(key, &DictionaryPayload{done: make(chan struct{})})
	payload := entry.(*DictionaryPayload)
	return func() (any, error) {
		<-payload.done
		return payload.res, payload.err
	}, !loaded
}

func (dc *dictionaryCache) set(path string, offset uint64, length uint32, res any, err error) {
	key := DictionaryKey{path, offset, length}
	entry, _ := dc.dictionaries.LoadOrStore(key, &DictionaryPayload{done: make(chan struct{})})
	payload := entry.(*DictionaryPayload)
	payload.res = res
	payload.err = err
	close(payload.done)
}
