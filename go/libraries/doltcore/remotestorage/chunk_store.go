// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dustin/go-humanize"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

var DownloadHedger *Hedger

func init() {
	// TODO: This does not necessarily respond well to changes in network
	// conditions during the program's runtime.
	DownloadHedger = NewHedger(
		8,
		NewMinStrategy(
			1*time.Second,
			NewPercentileStrategy(0, 1*time.Hour, 95.0),
		),
	)
}

var ErrUploadFailed = errors.New("upload failed")
var ErrInvalidDoltSpecPath = errors.New("invalid dolt spec path")

var globalHttpFetcher HTTPFetcher = &http.Client{}

var _ nbs.TableFileStore = (*DoltChunkStore)(nil)
var _ nbs.NBSCompressedChunkStore = (*DoltChunkStore)(nil)
var _ chunks.ChunkStore = (*DoltChunkStore)(nil)
var _ chunks.LoggingChunkStore = (*DoltChunkStore)(nil)

// We may need this to be configurable for users with really bad internet
var downThroughputCheck = iohelp.MinThroughputCheckParams{
	MinBytesPerSec: 1024,
	CheckInterval:  1 * time.Second,
	NumIntervals:   5,
}

const (
	downRetryCount   = 5
	uploadRetryCount = 5
)

var uploadRetryParams = backoff.NewExponentialBackOff()
var downRetryParams = backoff.NewExponentialBackOff()

func init() {
	uploadRetryParams.MaxInterval = 5 * time.Second

	downRetryParams.MaxInterval = 5 * time.Second
}

// Only hedge downloads of ranges < 4MB in length for now.
const HedgeDownloadSizeLimit = 4 * 1024 * 1024

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type ConcurrencyParams struct {
	ConcurrentSmallFetches int
	ConcurrentLargeFetches int
	LargeFetchSize         int
}

type DoltChunkStore struct {
	org         string
	repoName    string
	host        string
	csClient    remotesapi.ChunkStoreServiceClient
	cache       ChunkCache
	metadata    *remotesapi.GetRepoMetadataResponse
	nbf         *types.NomsBinFormat
	httpFetcher HTTPFetcher
	concurrency ConcurrencyParams
	stats       cacheStats
	logger      chunks.DebugLogger
}

func NewDoltChunkStoreFromPath(ctx context.Context, nbf *types.NomsBinFormat, path, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	tokens := strings.Split(strings.Trim(path, "/"), "/")
	if len(tokens) != 2 {
		return nil, ErrInvalidDoltSpecPath
	}

	// todo:
	// this may just be a dolthub thing.  Need to revisit how we do this.
	org := tokens[0]
	repoName := tokens[1]

	return NewDoltChunkStore(ctx, nbf, org, repoName, host, csClient)
}

func NewDoltChunkStore(ctx context.Context, nbf *types.NomsBinFormat, org, repoName, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	metadata, err := csClient.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{
		RepoId: &remotesapi.RepoId{
			Org:      org,
			RepoName: repoName,
		},
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	})

	if err != nil {
		return nil, err
	}

	return &DoltChunkStore{
		org:         org,
		repoName:    repoName,
		host:        host,
		csClient:    csClient,
		cache:       newMapChunkCache(),
		metadata:    metadata,
		nbf:         nbf,
		httpFetcher: globalHttpFetcher,
		concurrency: defaultConcurrency}, nil
}

func (dcs *DoltChunkStore) WithHTTPFetcher(fetcher HTTPFetcher) *DoltChunkStore {
	return &DoltChunkStore{
		org:      dcs.org,
		repoName: dcs.repoName,
		host:     dcs.host,
		csClient: dcs.csClient,
		cache:    dcs.cache, metadata: dcs.metadata, nbf: dcs.nbf, httpFetcher: fetcher, concurrency: dcs.concurrency,
		stats: dcs.stats}
}

func (dcs *DoltChunkStore) WithNoopChunkCache() *DoltChunkStore {
	return &DoltChunkStore{
		org:         dcs.org,
		repoName:    dcs.repoName,
		host:        dcs.host,
		csClient:    dcs.csClient,
		cache:       noopChunkCache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		concurrency: dcs.concurrency,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) WithChunkCache(cache ChunkCache) *DoltChunkStore {
	return &DoltChunkStore{
		org:         dcs.org,
		repoName:    dcs.repoName,
		host:        dcs.host,
		csClient:    dcs.csClient,
		cache:       cache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		concurrency: dcs.concurrency,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) WithDownloadConcurrency(concurrency ConcurrencyParams) *DoltChunkStore {
	return &DoltChunkStore{
		org:         dcs.org,
		repoName:    dcs.repoName,
		host:        dcs.host,
		csClient:    dcs.csClient,
		cache:       dcs.cache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		concurrency: concurrency,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) SetLogger(logger chunks.DebugLogger) {
	dcs.logger = logger
}

func (dcs *DoltChunkStore) logf(fmt string, args ...interface{}) {
	if dcs.logger != nil {
		dcs.logger.Logf(fmt, args...)
	}
}

func (dcs *DoltChunkStore) getRepoId() *remotesapi.RepoId {
	return &remotesapi.RepoId{
		Org:      dcs.org,
		RepoName: dcs.repoName,
	}
}

type cacheStats struct {
	Hits uint32
}

func (s cacheStats) CacheHits() uint32 {
	return s.Hits
}

type CacheStats interface {
	CacheHits() uint32
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (dcs *DoltChunkStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	hashes := hash.HashSet{h: struct{}{}}
	var found *chunks.Chunk
	err := dcs.GetMany(ctx, hashes, func(_ context.Context, c *chunks.Chunk) { found = c })
	if err != nil {
		return chunks.EmptyChunk, err
	}
	if found != nil {
		return *found, nil
	} else {
		return chunks.EmptyChunk, nil
	}
}

func (dcs *DoltChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	ae := atomicerr.New()
	decompressedSize := uint64(0)
	err := dcs.GetManyCompressed(ctx, hashes, func(ctx context.Context, cc nbs.CompressedChunk) {
		if ae.IsSet() {
			return
		}
		c, err := cc.ToChunk()
		if ae.SetIfErrAndCheck(err) {
			return
		}
		atomic.AddUint64(&decompressedSize, uint64(len(c.Data())))
		found(ctx, &c)
	})
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.LogKV("decompressed_bytes", decompressedSize)
	}
	if err != nil {
		return err
	}
	if err = ae.Get(); err != nil {
		return err
	}
	return nil
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (dcs *DoltChunkStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, nbs.CompressedChunk)) error {
	span, ctx := tracing.StartSpan(ctx, "remotestorage.GetManyCompressed")
	defer span.Finish()

	hashToChunk := dcs.cache.Get(hashes)

	span.LogKV("num_hashes", len(hashes), "cache_hits", len(hashToChunk))
	atomic.AddUint32(&dcs.stats.Hits, uint32(len(hashToChunk)))

	notCached := make([]hash.Hash, 0, len(hashes))
	for h := range hashes {
		c := hashToChunk[h]

		if c.IsEmpty() {
			notCached = append(notCached, h)
		} else {
			found(ctx, c)
		}
	}

	if len(notCached) > 0 {
		err := dcs.readChunksAndCache(ctx, hashes, notCached, found)

		if err != nil {
			return err
		}
	}

	return nil
}

const (
	getLocsBatchSize = 256
)

type GetRange remotesapi.HttpGetRange

func (gr *GetRange) ResourcePath() string {
	u, _ := url.Parse(gr.Url)
	return fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.Path)
}

func (gr *GetRange) Append(other *GetRange) {
	gr.Url = other.Url
	gr.Ranges = append(gr.Ranges, other.Ranges...)
}

func (gr *GetRange) Sort() {
	sort.Slice(gr.Ranges, func(i, j int) bool {
		return gr.Ranges[i].Offset < gr.Ranges[j].Offset
	})
}

func (gr *GetRange) ChunkStartOffset(i int) uint64 {
	return gr.Ranges[i].Offset
}

func (gr *GetRange) ChunkEndOffset(i int) uint64 {
	return gr.Ranges[i].Offset + uint64(gr.Ranges[i].Length)
}

func (gr *GetRange) GapBetween(i, j int) uint64 {
	return gr.ChunkStartOffset(j) - gr.ChunkEndOffset(i)
}

func (gr *GetRange) SplitAtGaps(maxGapBytes uint64) []*GetRange {
	gr.Sort()
	res := make([]*GetRange, 0)
	i := 0
	for i < len(gr.Ranges) {
		j := i + 1
		for j < len(gr.Ranges) {
			if gr.GapBetween(j-1, j) > maxGapBytes {
				break
			}
			if gr.GapBetween(i, j) > MaxFetchSize {
				break
			}
			j++
		}
		res = append(res, &GetRange{Url: gr.Url, Ranges: gr.Ranges[i:j]})
		i = j
	}
	return res
}

func (gr *GetRange) NumChunks() int {
	return len(gr.Ranges)
}

func (gr *GetRange) RangeLen() uint64 {
	return gr.ChunkEndOffset(gr.NumChunks()-1) - gr.ChunkStartOffset(0)
}

func (gr *GetRange) NumBytesInRanges() uint64 {
	res := uint64(0)
	for i := 0; i < len(gr.Ranges); i++ {
		start, end := gr.ChunkByteRange(i)
		res += start - end
	}
	return res
}

func (gr *GetRange) ChunkByteRange(i int) (uint64, uint64) {
	start := gr.ChunkStartOffset(i) - gr.ChunkStartOffset(0)
	end := gr.ChunkEndOffset(i) - gr.ChunkStartOffset(0)
	return start, end
}

func sortRangesBySize(ranges []*GetRange) {
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[j].RangeLen() < ranges[i].RangeLen()
	})
}

type resourcePathToUrlFunc func(ctx context.Context, lastError error, resourcePath string) (url string, err error)

func (gr *GetRange) GetDownloadFunc(ctx context.Context, stats StatsRecorder, fetcher HTTPFetcher, chunkChan chan nbs.CompressedChunk, pathToUrl resourcePathToUrlFunc) func() error {
	if len(gr.Ranges) == 0 {
		return func() error { return nil }
	}
	return func() error {
		urlF := func(lastError error) (string, error) {
			url, err := pathToUrl(ctx, lastError, gr.ResourcePath())
			if err != nil {
				return "", err
			}
			if url == "" {
				url = gr.Url
			}
			return url, nil
		}
		var comprData []byte
		var err error
		rangeLen := gr.RangeLen()
		if rangeLen > HedgeDownloadSizeLimit {
			comprData, err = rangeDownloadWithRetries(ctx, stats, fetcher, gr.ChunkStartOffset(0), rangeLen, 1, urlF)
		} else {
			comprData, err = hedgedRangeDownloadWithRetries(ctx, stats, fetcher, gr.ChunkStartOffset(0), rangeLen, urlF)
		}
		if err != nil {
			return err
		}
		// Send the chunk for each range included in GetRange.
		for i := 0; i < len(gr.Ranges); i++ {
			s, e := gr.ChunkByteRange(i)
			cmpChnk, err := nbs.NewCompressedChunk(hash.New(gr.Ranges[i].Hash), comprData[s:e])
			if err != nil {
				return err
			}
			select {
			case chunkChan <- cmpChnk:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

type locationRefresh struct {
	RefreshAfter   time.Time
	RefreshRequest *remotesapi.RefreshTableFileUrlRequest
	URL            string
	lastRefresh    time.Time
	mu             *sync.Mutex
}

func (r *locationRefresh) Add(resp *remotesapi.DownloadLoc) {
	if r.URL == "" {
		r.URL = resp.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url
	}
	if resp.RefreshAfter == nil {
		return
	}
	respTime := resp.RefreshAfter.AsTime()
	if (r.RefreshAfter == time.Time{}) || respTime.After(r.RefreshAfter) {
		r.RefreshAfter = resp.RefreshAfter.AsTime()
		r.RefreshRequest = resp.RefreshRequest
		r.URL = resp.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange.Url
	}
}

var refreshTableFileURLRetryDuration = 5 * time.Second

func (r *locationRefresh) GetURL(ctx context.Context, lastError error, client remotesapi.ChunkStoreServiceClient) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.RefreshRequest != nil {
		now := time.Now()
		wantsRefresh := now.After(r.RefreshAfter) || errors.Is(lastError, HttpError)
		canRefresh := time.Since(r.lastRefresh) > refreshTableFileURLRetryDuration
		if wantsRefresh && canRefresh {
			resp, err := client.RefreshTableFileUrl(ctx, r.RefreshRequest)
			if err != nil {
				return r.URL, err
			}
			r.RefreshAfter = resp.RefreshAfter.AsTime()
			r.URL = resp.Url
			r.lastRefresh = now
		}
	}
	return r.URL, nil
}

type dlLocations struct {
	ranges    map[string]*GetRange
	refreshes map[string]*locationRefresh
}

func newDlLocations() dlLocations {
	return dlLocations{
		ranges:    make(map[string]*GetRange),
		refreshes: make(map[string]*locationRefresh),
	}
}

func (l *dlLocations) Add(resp *remotesapi.DownloadLoc) {
	gr := (*GetRange)(resp.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange)
	path := gr.ResourcePath()
	if v, ok := l.ranges[path]; ok {
		v.Append(gr)
		l.refreshes[path].Add(resp)
	} else {
		l.ranges[path] = gr
		refresh := &locationRefresh{mu: new(sync.Mutex)}
		refresh.Add(resp)
		l.refreshes[path] = refresh
	}
}

func (dcs *DoltChunkStore) getDLLocs(ctx context.Context, hashes []hash.Hash) (dlLocations, error) {
	span, ctx := tracing.StartSpan(ctx, "remotestorage.getDLLocs")
	span.LogKV("num_hashes", len(hashes))
	defer span.Finish()

	res := newDlLocations()

	// channel for receiving results from go routines making grpc calls to get download locations for chunks
	resCh := make(chan []*remotesapi.DownloadLoc)

	eg, ctx := errgroup.WithContext(ctx)

	// go routine for receiving the results of the grpc calls and aggregating the results into resourceToUrlAndRanges
	eg.Go(func() error {
		for {
			select {
			case locs, ok := <-resCh:
				if !ok {
					return nil
				}
				for _, loc := range locs {
					res.Add(loc)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// go routine for batching the get location requests, streaming the requests and streaming the responses.
	eg.Go(func() error {
		var reqs []*remotesapi.GetDownloadLocsRequest
		hashesBytes := HashesToSlices(hashes)
		batchItr(len(hashesBytes), getLocsBatchSize, func(st, end int) (stop bool) {
			batch := hashesBytes[st:end]
			req := &remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), ChunkHashes: batch}
			reqs = append(reqs, req)
			return false
		})
		op := func() error {
			seg, ctx := errgroup.WithContext(ctx)
			stream, err := dcs.csClient.StreamDownloadLocations(ctx)
			if err != nil {
				return NewRpcError(err, "StreamDownloadLocations", dcs.host, nil)
			}
			completedReqs := 0
			// Write requests
			seg.Go(func() error {
				for i := range reqs {
					if err := stream.Send(reqs[i]); err != nil {
						return NewRpcError(err, "StreamDownloadLocations", dcs.host, reqs[i])
					}
				}
				return stream.CloseSend()
			})
			// Read responses
			seg.Go(func() error {
				for {
					resp, err := stream.Recv()
					if err != nil {
						if err == io.EOF {
							return nil
						}
						return NewRpcError(err, "StreamDownloadLocations", dcs.host, reqs[completedReqs])
					}
					select {
					case resCh <- resp.Locs:
						completedReqs += 1
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			})
			err = seg.Wait()
			reqs = reqs[completedReqs:]
			if len(reqs) == 0 {
				close(resCh)
			}
			return processGrpcErr(err)
		}
		return backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))
	})

	if err := eg.Wait(); err != nil {
		return dlLocations{}, err
	}
	return res, nil
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes hash.HashSet, notCached []hash.Hash, found func(context.Context, nbs.CompressedChunk)) error {
	// get the locations where the chunks can be downloaded from
	dlLocs, err := dcs.getDLLocs(ctx, notCached)
	if err != nil {
		return err
	}

	// channel to receive chunks on
	chunkChan := make(chan nbs.CompressedChunk, 128)

	toSend := make(map[hash.Hash]struct{}, len(notCached))
	for _, h := range notCached {
		toSend[h] = struct{}{}
	}

	eg, egCtx := errgroup.WithContext(ctx)
	// start a go routine to receive the downloaded chunks on
	eg.Go(func() error {
		for {
			select {
			case chunk, ok := <-chunkChan:
				if !ok {
					return nil
				}
				if dcs.cache.PutChunk(chunk) {
					return errors.New("too much data...")
				}
				h := chunk.Hash()

				if _, send := toSend[h]; send {
					found(egCtx, chunk)
				}
			case <-egCtx.Done():
				return nil
			}
		}
	})

	// download the chunks and close the channel after
	eg.Go(func() error {
		defer close(chunkChan)
		return dcs.downloadChunks(egCtx, dlLocs, chunkChan)
	})

	// wait for all the results to finish processing
	return eg.Wait()
}

// Returns true iff the value at the address |h| is contained in the
// store
func (dcs *DoltChunkStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	hashes := hash.HashSet{h: struct{}{}}
	absent, err := dcs.HasMany(ctx, hashes)

	if err != nil {
		return false, err
	}

	return len(absent) == 0, nil
}

const maxHasManyBatchSize = 16 * 1024

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (dcs *DoltChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	// get the set of hashes that isn't already in the cache
	notCached := dcs.cache.Has(hashes)

	if len(notCached) == 0 {
		return notCached, nil
	}

	// convert the set to a slice of hashes and a corresponding slice of the byte encoding for those hashes
	hashSl, byteSl := HashSetToSlices(notCached)

	absent := make(hash.HashSet)
	var found []nbs.CompressedChunk
	var err error

	batchItr(len(hashSl), maxHasManyBatchSize, func(st, end int) (stop bool) {
		// slice the slices into a batch of hashes
		currHashSl := hashSl[st:end]
		currByteSl := byteSl[st:end]

		// send a request to the remote api to determine which chunks the remote api already has
		req := &remotesapi.HasChunksRequest{RepoId: dcs.getRepoId(), Hashes: currByteSl}
		var resp *remotesapi.HasChunksResponse
		resp, err = dcs.csClient.HasChunks(ctx, req)
		if err != nil {
			err = NewRpcError(err, "HasChunks", dcs.host, req)
			return true
		}

		numAbsent := len(resp.Absent)
		sort.Slice(resp.Absent, func(i, j int) bool {
			return resp.Absent[i] < resp.Absent[j]
		})

		// loop over every hash in the current batch, and if they are absent from the remote host add them to the
		// absent set, otherwise append them to the found slice
		for i, j := 0, 0; i < len(currHashSl); i++ {
			currHash := currHashSl[i]

			nextAbsent := -1
			if j < numAbsent {
				nextAbsent = int(resp.Absent[j])
			}

			if i == nextAbsent {
				absent[currHash] = struct{}{}
				j++
			} else {
				c := nbs.ChunkToCompressedChunk(chunks.NewChunkWithHash(currHash, []byte{}))
				found = append(found, c)
			}
		}

		return false
	})

	if err != nil {
		return nil, err
	}

	if len(found)+len(absent) != len(notCached) {
		panic("not all chunks were accounted for")
	}

	if len(found) > 0 {
		if dcs.cache.Put(found) {
			return hash.HashSet{}, errors.New("too much data")
		}
	}

	return absent, nil
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	cc := nbs.ChunkToCompressedChunk(c)
	if dcs.cache.Put([]nbs.CompressedChunk{cc}) {
		return errors.New("too much data")
	}
	return nil
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (dcs *DoltChunkStore) Version() string {
	return dcs.metadata.NbfVersion
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (dcs *DoltChunkStore) Rebase(ctx context.Context) error {
	req := &remotesapi.RebaseRequest{RepoId: dcs.getRepoId()}
	_, err := dcs.csClient.Rebase(ctx, req)

	if err != nil {
		return NewRpcError(err, "Rebase", dcs.host, req)
	}

	return dcs.refreshRepoMetadata(ctx)
}

func (dcs *DoltChunkStore) refreshRepoMetadata(ctx context.Context) error {
	mdReq := &remotesapi.GetRepoMetadataRequest{
		RepoId: &remotesapi.RepoId{
			Org:      dcs.org,
			RepoName: dcs.repoName,
		},
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}
	metadata, err := dcs.csClient.GetRepoMetadata(ctx, mdReq)
	if err != nil {
		return NewRpcError(err, "GetRepoMetadata", dcs.host, mdReq)
	}
	dcs.metadata = metadata
	return nil
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (dcs *DoltChunkStore) Root(ctx context.Context) (hash.Hash, error) {
	req := &remotesapi.RootRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.Root(ctx, req)

	if err != nil {
		return hash.Hash{}, NewRpcError(err, "Root", dcs.host, req)
	}

	return hash.New(resp.RootHash), nil
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (dcs *DoltChunkStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	hashToChunkCount, err := dcs.uploadChunks(ctx)

	if err != nil {
		return false, err
	}

	chnkTblInfo := make([]*remotesapi.ChunkTableInfo, 0, len(hashToChunkCount))
	for h, cnt := range hashToChunkCount {
		chnkTblInfo = append(chnkTblInfo, &remotesapi.ChunkTableInfo{Hash: h[:], ChunkCount: uint32(cnt)})
	}

	req := &remotesapi.CommitRequest{
		RepoId:         dcs.getRepoId(),
		Current:        current[:],
		Last:           last[:],
		ChunkTableInfo: chnkTblInfo,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}
	resp, err := dcs.csClient.Commit(ctx, req)
	if err != nil {
		return false, NewRpcError(err, "Commit", dcs.host, req)
	}

	return resp.Success, dcs.refreshRepoMetadata(ctx)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (dcs *DoltChunkStore) Stats() interface{} {
	return cacheStats{atomic.LoadUint32(&dcs.stats.Hits)}
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (dcs *DoltChunkStore) StatsSummary() string {
	return fmt.Sprintf("CacheHits: %v", dcs.Stats().(CacheStats).CacheHits())
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (dcs *DoltChunkStore) Close() error {
	return nil
}

// getting this working using the simplest approach first
func (dcs *DoltChunkStore) uploadChunks(ctx context.Context) (map[hash.Hash]int, error) {
	hashToChunk := dcs.cache.GetAndClearChunksToFlush()

	if len(hashToChunk) == 0 {
		return map[hash.Hash]int{}, nil
	}

	chnks := make([]chunks.Chunk, 0, len(hashToChunk))
	for _, chable := range hashToChunk {
		ch, err := chable.ToChunk()

		if err != nil {
			return nil, err
		}

		chnks = append(chnks, ch)
	}

	hashToCount := make(map[hash.Hash]int)
	hashToData := make(map[hash.Hash][]byte)
	hashToDetails := make(map[hash.Hash]*remotesapi.TableFileDetails)

	// structuring so this can be done as multiple files in the future.
	{
		name, data, err := nbs.WriteChunks(chnks)

		if err != nil {
			return map[hash.Hash]int{}, err
		}

		h := hash.Parse(name)
		hashToData[h] = data
		hashToCount[h] = len(chnks)

		md5Bytes := md5.Sum(data)
		hashToDetails[h] = &remotesapi.TableFileDetails{
			Id:            h[:],
			ContentLength: uint64(len(data)),
			ContentHash:   md5Bytes[:],
		}
	}

	tfds := make([]*remotesapi.TableFileDetails, 0, len(hashToDetails))
	for _, v := range hashToDetails {
		tfds = append(tfds, v)
	}

	req := &remotesapi.GetUploadLocsRequest{RepoId: dcs.getRepoId(), TableFileDetails: tfds}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return map[hash.Hash]int{}, NewRpcError(err, "GetUploadLocations", dcs.host, req)
	}

	for _, loc := range resp.Locs {
		var err error
		h := hash.New(loc.TableFileHash)
		data := hashToData[h]
		details := hashToDetails[h]
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:
			err = dcs.httpPostUpload(ctx, loc.TableFileHash, typedLoc.HttpPost, bytes.NewBuffer(data), details.ContentHash)
		default:
			break
		}

		if err != nil {
			return map[hash.Hash]int{}, err
		}
	}

	return hashToCount, nil
}

type Sizer interface {
	Size() int64
}

func (dcs *DoltChunkStore) httpPostUpload(ctx context.Context, hashBytes []byte, post *remotesapi.HttpPostTableFile, rd io.Reader, contentHash []byte) error {
	return HttpPostUpload(ctx, dcs.httpFetcher, post, rd, contentHash)
}

func HttpPostUpload(ctx context.Context, httpFetcher HTTPFetcher, post *remotesapi.HttpPostTableFile, rd io.Reader, contentHash []byte) error {
	req, err := http.NewRequest(http.MethodPut, post.Url, rd)
	if err != nil {
		return err
	}

	if sizer, ok := rd.(Sizer); ok {
		req.ContentLength = sizer.Size()
	}

	if len(contentHash) > 0 {
		md5s := base64.StdEncoding.EncodeToString(contentHash)
		req.Header.Set("Content-MD5", md5s)
	}

	fetcher := globalHttpFetcher
	if httpFetcher != nil {
		fetcher = httpFetcher
	}

	var resp *http.Response
	op := func() error {
		var err error
		resp, err = fetcher.Do(req.WithContext(ctx))

		if err == nil {
			defer func() {
				_ = resp.Body.Close()
			}()
		}

		return processHttpResp(resp, err)
	}

	err = backoff.Retry(op, backoff.WithMaxRetries(uploadRetryParams, uploadRetryCount))

	if err != nil {
		return err
	}

	return nil
}

// aggregateDownloads looks for byte ranges that need to be downloaded, and tries to aggregate them into a smaller number
// of larger downloads.  It does this by sorting the ranges of bytes that are needed, and then comparing how close together
// neighboring ranges are.  If they are within the threshold the two ranges will be aggregated into a single request for
// the entire range of data.
func aggregateDownloads(aggDistance uint64, resourceGets map[string]*GetRange) []*GetRange {
	var res []*GetRange
	for _, resourceGet := range resourceGets {
		res = append(res, resourceGet.SplitAtGaps(aggDistance)...)
	}
	return res
}

const (
	chunkAggDistance = 8 * 1024
)

const MaxFetchSize = 128 * 1024 * 1024

var defaultConcurrency ConcurrencyParams = ConcurrencyParams{
	ConcurrentSmallFetches: 64,
	ConcurrentLargeFetches: 2,
	LargeFetchSize:         2 * 1024 * 1024,
}

func logDownloadStats(span opentracing.Span, originalGets map[string]*GetRange, computedGets []*GetRange) {
	chunkCount := 0
	originalBytes := uint64(0)
	for _, r := range originalGets {
		chunkCount += r.NumChunks()
		originalBytes += r.NumBytesInRanges()
	}
	downloadBytes := uint64(0)
	for _, r := range computedGets {
		downloadBytes += r.RangeLen()
	}
	span.LogKV("num_files", len(originalGets), "num_chunks", chunkCount, "num_batches", len(computedGets), "original_bytes", originalBytes, "download_bytes", downloadBytes)
}

// creates work functions for each download and executes them in parallel.  The work functions write downloaded chunks
// to chunkChan
func (dcs *DoltChunkStore) downloadChunks(ctx context.Context, dlLocs dlLocations, chunkChan chan nbs.CompressedChunk) error {
	span, ctx := tracing.StartSpan(ctx, "remotestorage.downloadChunks")
	defer span.Finish()

	resourceGets := dlLocs.ranges

	gets := aggregateDownloads(chunkAggDistance, resourceGets)
	logDownloadStats(span, resourceGets, gets)

	sortRangesBySize(gets)

	toUrl := func(ctx context.Context, lastError error, resourcePath string) (string, error) {
		return dlLocs.refreshes[resourcePath].GetURL(ctx, lastError, dcs.csClient)
	}

	stats := StatsFactory()

	eg, ctx := errgroup.WithContext(ctx)

	// loop over all the gets that need to be downloaded and create a work function for each
	work := make([]func() error, len(gets))
	largeCutoff := -1
	for i, get := range gets {
		work[i] = get.GetDownloadFunc(ctx, stats, dcs.httpFetcher, chunkChan, toUrl)
		if get.RangeLen() >= uint64(dcs.concurrency.LargeFetchSize) {
			largeCutoff = i
		}
	}

	// execute the work
	eg.Go(func() error {
		return concurrentExec(work[0:largeCutoff+1], dcs.concurrency.ConcurrentLargeFetches)
	})
	eg.Go(func() error {
		return concurrentExec(work[largeCutoff+1:len(work)], dcs.concurrency.ConcurrentSmallFetches)
	})

	defer func() {
		StatsFlusher(stats)
	}()
	return eg.Wait()
}

type urlFactoryFunc func(error) (string, error)

func hedgedRangeDownloadWithRetries(ctx context.Context, stats StatsRecorder, fetcher HTTPFetcher, offset, length uint64, urlStrF urlFactoryFunc) ([]byte, error) {
	res, err := DownloadHedger.Do(ctx, Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			return rangeDownloadWithRetries(ctx, stats, fetcher, offset, length, n, urlStrF)
		},
		Size: int(length),
	})
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

// rangeDownloadWithRetries executes an http get with the 'Range' header to get a range of bytes from a file.  Request
// is executed with retries and if progress was made, downloads will be resumed from where they left off on subsequent attempts.
func rangeDownloadWithRetries(ctx context.Context, stats StatsRecorder, fetcher HTTPFetcher, offset, length uint64, hedgeN int, urlStrF urlFactoryFunc) ([]byte, error) {
	// create the request

	// parameters used for resuming downloads.
	var allBufs [][]byte
	currOffset := offset
	currLength := length

	var lastError error
	var retryCnt int

	//execute the request
	op := func() (rerr error) {
		defer func() {
			lastError = rerr
			retryCnt += 1
		}()
		urlStr, err := urlStrF(lastError)
		if err != nil {
			return err
		}

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		if err != nil {
			return err
		}

		rangeVal := fmt.Sprintf("bytes=%d-%d", currOffset, currOffset+currLength-1)
		req.Header.Set("Range", rangeVal)

		stats.RecordDownloadAttemptStart(hedgeN, retryCnt, currOffset-offset, length)
		start := time.Now()
		resp, err := fetcher.Do(req.WithContext(ctx))
		if err == nil {
			defer func() {
				_ = resp.Body.Close()
			}()
		}

		respErr := processHttpResp(resp, err)
		if respErr != nil {
			return respErr
		}
		stats.RecordTimeToFirstByte(hedgeN, retryCnt, length, time.Since(start))

		// read the results
		comprData, err := iohelp.ReadWithMinThroughput(resp.Body, int64(currLength), downThroughputCheck)

		dataRead := len(comprData)
		if dataRead > 0 {
			allBufs = append(allBufs, comprData)
			currLength -= uint64(dataRead)
			currOffset += uint64(dataRead)
		}
		return err
	}

	dstart := time.Now()
	err := backoff.Retry(op, backoff.WithMaxRetries(downRetryParams, downRetryCount))
	if err != nil {
		return nil, err
	}
	stats.RecordDownloadComplete(hedgeN, retryCnt, length, time.Since(dstart))

	return collapseBuffers(allBufs, length), nil
}

func collapseBuffers(bufs [][]byte, length uint64) []byte {
	if len(bufs) == 1 {
		return bufs[0]
	}
	res := make([]byte, 0, length)
	for _, buf := range bufs {
		res = append(res, buf...)
	}
	return res
}

func (dcs *DoltChunkStore) SupportedOperations() nbs.TableFileStoreOps {
	return nbs.TableFileStoreOps{
		CanRead:  true,
		CanWrite: true,
		CanPrune: false,
		CanGC:    false,
	}
}

// WriteTableFile reads a table file from the provided reader and writes it to the chunk store.
func (dcs *DoltChunkStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	dcs.logf("getting upload location for file %s with %d chunks and size %s", fileId, numChunks, humanize.Bytes(contentLength))

	fileIdBytes := hash.Parse(fileId)
	tfd := &remotesapi.TableFileDetails{
		Id:            fileIdBytes[:],
		ContentLength: contentLength,
		ContentHash:   contentHash,
	}
	req := &remotesapi.GetUploadLocsRequest{
		RepoId:           dcs.getRepoId(),
		TableFileDetails: []*remotesapi.TableFileDetails{tfd},

		// redundant and deprecated.  Still setting for compatibility, but will remove "promptly".
		TableFileHashes: [][]byte{fileIdBytes[:]},
	}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return NewRpcError(err, "GetUploadLocations", dcs.host, req)
	}

	if len(resp.Locs) != 1 {
		return errors.New("unexpected upload location count")
	}

	loc := resp.Locs[0]
	switch typedLoc := loc.Location.(type) {
	case *remotesapi.UploadLoc_HttpPost:
		urlStr := typedLoc.HttpPost.Url

		// strip off the query parameters as they clutter the logs. We only really care about being able to verify the table
		// files are being uploaded to the correct places on S3.
		qmIdx := strings.IndexRune(urlStr, '?')
		if qmIdx != -1 {
			urlStr = urlStr[:qmIdx]
		}
		dcs.logf("uploading %s to %s", fileId, urlStr)

		err = dcs.httpPostUpload(ctx, loc.TableFileHash, typedLoc.HttpPost, rd, contentHash)

		if err != nil {
			dcs.logf("failed to upload %s to %s. err: %s", fileId, urlStr, err.Error())
			return err
		}

		dcs.logf("successfully uploaded %s to %s", fileId, urlStr)

	default:
		return errors.New("unsupported upload location")
	}

	return nil
}

// AddTableFilesToManifest adds table files to the manifest
func (dcs *DoltChunkStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error {
	chnkTblInfo := make([]*remotesapi.ChunkTableInfo, 0, len(fileIdToNumChunks))

	debugStr := ""
	for fileId, numChunks := range fileIdToNumChunks {
		debugStr += fmt.Sprintln(fileId, ":", numChunks)
		fileIdBytes := hash.Parse(fileId)
		chnkTblInfo = append(chnkTblInfo, &remotesapi.ChunkTableInfo{Hash: fileIdBytes[:], ChunkCount: uint32(numChunks)})
	}

	dcs.logf("Adding Table files to repo: %s/%s -\n%s", dcs.getRepoId().Org, dcs.getRepoId().RepoName, debugStr)
	atReq := &remotesapi.AddTableFilesRequest{
		RepoId:         dcs.getRepoId(),
		ChunkTableInfo: chnkTblInfo,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}

	atResp, err := dcs.csClient.AddTableFiles(ctx, atReq)

	if err != nil {
		return NewRpcError(err, "AddTableFiles", dcs.host, atReq)
	}

	if !atResp.Success {
		return errors.New("update table files failed")
	}

	return nil
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (dcs *DoltChunkStore) PruneTableFiles(ctx context.Context) error {
	return chunks.ErrUnsupportedOperation
}

// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files)
// and a list of only appendix table files
func (dcs *DoltChunkStore) Sources(ctx context.Context) (hash.Hash, []nbs.TableFile, []nbs.TableFile, error) {
	req := &remotesapi.ListTableFilesRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.ListTableFiles(ctx, req)
	if err != nil {
		return hash.Hash{}, nil, nil, NewRpcError(err, "ListTableFiles", dcs.host, req)
	}
	sourceFiles := getTableFiles(dcs, resp.TableFileInfo)
	appendixFiles := getTableFiles(dcs, resp.AppendixTableFileInfo)
	return hash.New(resp.RootHash), sourceFiles, appendixFiles, nil
}

func getTableFiles(dcs *DoltChunkStore, infoList []*remotesapi.TableFileInfo) []nbs.TableFile {
	tableFiles := make([]nbs.TableFile, 0)
	for _, nfo := range infoList {
		tableFiles = append(tableFiles, DoltRemoteTableFile{dcs, nfo})
	}
	return tableFiles
}

func (dcs *DoltChunkStore) Size(ctx context.Context) (uint64, error) {
	return dcs.metadata.StorageSize, nil
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (dcs *DoltChunkStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	panic("Not Implemented")
}

// DoltRemoteTableFile is an implementation of a TableFile that lives in a DoltChunkStore
type DoltRemoteTableFile struct {
	dcs  *DoltChunkStore
	info *remotesapi.TableFileInfo
}

// FileID gets the id of the file
func (drtf DoltRemoteTableFile) FileID() string {
	return drtf.info.FileId
}

// NumChunks returns the number of chunks in a table file
func (drtf DoltRemoteTableFile) NumChunks() int {
	return int(drtf.info.NumChunks)
}

var ErrRemoteTableFileGet = errors.New("HTTP GET for remote table file failed")

func sanitizeSignedUrl(url string) string {
	si := strings.Index(url, "Signature=")
	if si == -1 {
		return url
	}
	ei := strings.Index(url[si:], "&")
	if ei == -1 {
		return url[:si+15] + "..."
	} else {
		return url[:si+15] + "..." + url[si:][ei:]
	}
}

// Open returns an io.ReadCloser which can be used to read the bytes of a table file.
func (drtf DoltRemoteTableFile) Open(ctx context.Context) (io.ReadCloser, uint64, error) {
	if drtf.info.RefreshAfter != nil && drtf.info.RefreshAfter.AsTime().After(time.Now()) {
		resp, err := drtf.dcs.csClient.RefreshTableFileUrl(ctx, drtf.info.RefreshRequest)
		if err == nil {
			drtf.info.Url = resp.Url
			drtf.info.RefreshAfter = resp.RefreshAfter
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, drtf.info.Url, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := drtf.dcs.httpFetcher.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body := make([]byte, 4096)
		n, _ := io.ReadFull(resp.Body, body)
		return nil, 0, fmt.Errorf("%w: status code: %d;\nurl: %s\n\nbody:\n\n%s\n", ErrRemoteTableFileGet, resp.StatusCode, sanitizeSignedUrl(drtf.info.Url), string(body[0:n]))
	}

	return resp.Body, uint64(resp.ContentLength), nil
}
