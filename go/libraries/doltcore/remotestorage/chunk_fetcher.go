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

	"github.com/cenkalti/backoff/v4"
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
	// and as refs are found, they will be added to the channel for workers to batch and request. NM4.
	toGetCh chan hash.HashSet

	// resCh is the results channel for the fetcher. It is used both to return
	// chunks themselves, and to indicate which chunks were requested but missing
	// buy having a Hash, but are empty. NM4.
	resCh chan nbs.ToChunker

	abortCh chan struct{}
	stats   StatsRecorder
}

const (
	getLocsBatchSize = 512

	reliableCallReadRequestTimeout = 15 * time.Second
	reliableCallDeliverRespTimeout = 15 * time.Second
)

var globalDictCache *DictionaryCache
var once sync.Once

func NewChunkFetcher(ctx context.Context, dcs *DoltChunkStore) *ChunkFetcher {
	once.Do(func() {
		globalDictCache = NewDictionaryCache(newDownloads(), dcs.csClient)
	})

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

type fetchResp struct {
	get     *GetRange
	refresh func(ctx context.Context, err error, client remotesapi.ChunkStoreServiceClient) (string, error)
}

type fetchReq struct {
	respCh   chan fetchResp
	cancelCh chan struct{}
}

// A simple structure to keep track of *GetRange requests along with
// |locationRefreshes| for the URL paths we have seen.
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
		// NM4 - Split at this point? Break the dictionary into its own request.
		d.ranges.Insert(gr.Url, r.Hash[:], r.Offset, r.Length, r.DictionaryOffset, r.DictionaryLength)
		//		if r.DictionaryLength == 0 {
		//			// NM4 - maybe invert the hash, and add it to a set of..... not sure.
		//			d.ranges.Insert(gr.Url, r.Hash, r.DictionaryOffset, r.DictionaryLength)
		//		}
	}
}

// NM4 - On the client side, we only request HttpRanges for raw bytes. The struct includes the dictionary offset and length,
// but those only make sense in the response of DownloadLocations.
func toGetRange(rs []*ranges.GetRange) *GetRange {
	ret := new(GetRange)
	for _, r := range rs {
		ret.Url = r.Url
		ret.Ranges = append(ret.Ranges, &remotesapi.RangeChunk{
			Hash:             r.Hash,
			Offset:           r.Offset,
			Length:           r.Length,
			DictionaryOffset: r.DictionaryOffset,
			DictionaryLength: r.DictionaryLength,
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
		// pending is our slice of request threads that showed up
		// asking for a download. We range through it and try to send
		// them any work we have available.
		for j := range pending {
			// |toSend| could have come from a previous iteration
			// of this loop or the outer loop. If it's |nil|, we
			// can get the next range to download from
			// |downloads.ranges|.
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
				// Because of dynamic thread pool sizing, a
				// request thread could have been canceled and
				// it has now gone away. If this happens, its
				// respCh will be set to |nil| below and we
				// will remove it from our |pending| set. But
				// we need to hold onto |toSend| so that we do
				// send it to a request thread eventually.
			case <-ctx.Done():
				return context.Cause(ctx)
			}

			pending[j].respCh = nil
		}

		// Remove anything from |pending| that was actually delivered
		// to. We use |respCh == nil| to indicate that the above loop
		// delivered to the download thread.
		newpending := make([]fetchReq, 0)
		for i := range pending {
			if pending[i].respCh != nil {
				newpending = append(newpending, pending[i])
			}
		}
		pending = newpending

		// Once |locCh| closes, we set |locCh| to nil. If |locCh| is
		// nil and our ranges Tree is empty, then we have delivered
		// every download we will ever see to a download thread. We can
		// close |doneCh| and return nil.
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

func fetcherDownloadURLThread(ctx context.Context, fetchReqCh chan fetchReq, doneCh <-chan struct{}, chunkCh chan nbs.ToChunker, client remotesapi.ChunkStoreServiceClient, stats StatsRecorder, health reliable.HealthRecorder, fetcher HTTPFetcher, params NetworkRequestParams) error {
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

///////

type DictionaryKey struct {
	url string
	off uint64
	len uint32
}

type DictionaryCache struct {
	mu     sync.Mutex
	cache  map[DictionaryKey]*gozstd.DDict
	client remotesapi.ChunkStoreServiceClient
	dlds   downloads
}

func NewDictionaryCache(downloads downloads, client remotesapi.ChunkStoreServiceClient) *DictionaryCache {
	return &DictionaryCache{
		mu:     sync.Mutex{},
		cache:  make(map[DictionaryKey]*gozstd.DDict),
		client: client,
		dlds:   downloads,
	}
}

func (dc *DictionaryCache) Get(rang *GetRange, idx int, stats StatsRecorder, recorder reliable.HealthRecorder) (*gozstd.DDict, error) {
	// Way too granular... but I'll use a real cache for production. prototype maddddddneeesssss
	dc.mu.Lock()
	defer dc.mu.Unlock()

	path := rang.ResourcePath()
	off := rang.Ranges[idx].DictionaryOffset
	ln := rang.Ranges[idx].DictionaryLength

	key := DictionaryKey{path, off, ln}
	if v, ok := dc.cache[key]; ok {
		return v, nil
	} else {

		pathToUrl := dc.dlds.refreshes[path]
		if pathToUrl == nil {
			// Kinda do what Add does....
			refresh := new(locationRefresh)

			sRang := &remotesapi.HttpGetRange{}
			sRang.Url = rang.Url
			sRang.Ranges = append(sRang.Ranges, &remotesapi.RangeChunk{Offset: off, Length: ln})
			rang := &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: sRang}
			dl := &remotesapi.DownloadLoc{Location: rang}

			refresh.Add(dl)
			dc.dlds.refreshes[path] = refresh

			pathToUrl = refresh
		}

		ctx := context.Background()
		fetcher := globalHttpFetcher

		urlF := func(lastError error) (string, error) {
			earl, err := pathToUrl.GetURL(ctx, lastError, dc.client)
			if err != nil {
				return "", err
			}
			if earl == "" {
				earl = path
			}
			return earl, nil
		}

		resp := reliable.StreamingRangeDownload(ctx, reliable.StreamingRangeRequest{
			Fetcher: fetcher,
			Offset:  off,
			Length:  uint64(ln),
			UrlFact: urlF,
			Stats:   stats,
			Health:  recorder,
			BackOffFact: func(ctx context.Context) backoff.BackOff {
				return downloadBackOff(ctx, 3) // params.DownloadRetryCount)
			},
			Throughput: reliable.MinimumThroughputCheck{
				CheckInterval: defaultRequestParams.ThroughputMinimumCheckInterval,
				BytesPerCheck: defaultRequestParams.ThroughputMinimumBytesPerCheck,
				NumIntervals:  defaultRequestParams.ThroughputMinimumNumIntervals,
			},
			RespHeadersTimeout: defaultRequestParams.RespHeadersTimeout,
		})
		defer resp.Close()

		buf := make([]byte, ln)
		_, err := io.ReadFull(resp.Body, buf)
		if err != nil {
			return nil, err
		}

		rawDict, err := gozstd.Decompress(nil, buf)
		if err != nil {
			return nil, err
		}

		dict, err := gozstd.NewDDict(rawDict)
		if err != nil {
			return nil, err
		}

		dc.cache[key] = dict
		return dict, nil
	}
}
