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
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/reliable"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrCacheCapacityExceeded = errors.New("too much data: the cache capacity has been reached")

var ErrUploadFailed = errors.New("upload failed")

var defaultDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

var defaultTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DialContext:           defaultDialer.DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          1024,
	MaxIdleConnsPerHost:   256,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var globalHttpFetcher HTTPFetcher = &http.Client{
	Transport: defaultTransport,
}

var _ chunks.TableFileStore = (*DoltChunkStore)(nil)
var _ nbs.NBSCompressedChunkStore = (*DoltChunkStore)(nil)
var _ chunks.ChunkStore = (*DoltChunkStore)(nil)
var _ chunks.LoggingChunkStore = (*DoltChunkStore)(nil)

var tracer = otel.Tracer("github.com/dolthub/dolt/go/libraries/doltcore/remotestorage")

func uploadBackOff(ctx context.Context, max int) backoff.BackOff {
	ret := backoff.NewExponentialBackOff()
	ret.MaxInterval = 5 * time.Second
	return backoff.WithContext(backoff.WithMaxRetries(ret, uint64(max)), ctx)
}

func downloadBackOff(ctx context.Context, max int) backoff.BackOff {
	ret := backoff.NewExponentialBackOff()
	ret.MaxInterval = 5 * time.Second
	return backoff.WithContext(backoff.WithMaxRetries(ret, uint64(max)), ctx)
}

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type NetworkRequestParams struct {
	StartingConcurrentDownloads    int
	MaximumConcurrentDownloads     int
	UploadRetryCount               int
	DownloadRetryCount             int
	ThroughputMinimumCheckInterval time.Duration
	ThroughputMinimumBytesPerCheck int
	ThroughputMinimumNumIntervals  int
	RespHeadersTimeout             time.Duration
}

var defaultRequestParams = NetworkRequestParams{
	StartingConcurrentDownloads:    64,
	MaximumConcurrentDownloads:     64,
	UploadRetryCount:               5,
	DownloadRetryCount:             5,
	ThroughputMinimumCheckInterval: time.Second,
	ThroughputMinimumBytesPerCheck: 1024,
	ThroughputMinimumNumIntervals:  5,
	RespHeadersTimeout:             15 * time.Second,
}

type DoltChunkStore struct {
	repoId      *remotesapi.RepoId
	repoPath    string
	repoToken   *atomic.Value // string
	host        string
	root        hash.Hash
	csClient    remotesapi.ChunkStoreServiceClient
	finalizer   func() error
	cache       ChunkCache
	metadata    *remotesapi.GetRepoMetadataResponse
	nbf         *types.NomsBinFormat
	httpFetcher HTTPFetcher
	params      NetworkRequestParams
	stats       cacheStats
	logger      chunks.DebugLogger
	wsValidate  bool
}

func NewDoltChunkStoreFromPath(ctx context.Context, nbf *types.NomsBinFormat, path, host string, wsval bool, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	var repoId *remotesapi.RepoId

	path = strings.Trim(path, "/")
	tokens := strings.Split(path, "/")
	if len(tokens) == 2 {
		org := tokens[0]
		repoName := tokens[1]
		repoId = &remotesapi.RepoId{
			Org:      org,
			RepoName: repoName,
		}
	}

	metadata, err := csClient.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{
		RepoId:   repoId,
		RepoPath: path,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	})
	if err != nil {
		return nil, err
	}

	repoToken := new(atomic.Value)
	if metadata.RepoToken != "" {
		repoToken.Store(metadata.RepoToken)
	}

	cs := &DoltChunkStore{
		repoId:      repoId,
		repoPath:    path,
		repoToken:   repoToken,
		host:        host,
		csClient:    csClient,
		finalizer:   func() error { return nil },
		cache:       newMapChunkCache(),
		metadata:    metadata,
		nbf:         nbf,
		httpFetcher: globalHttpFetcher,
		params:      defaultRequestParams,
		wsValidate:  wsval,
	}
	err = cs.loadRoot(ctx)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func (dcs *DoltChunkStore) WithHTTPFetcher(fetcher HTTPFetcher) *DoltChunkStore {
	return &DoltChunkStore{
		repoId:      dcs.repoId,
		repoPath:    dcs.repoPath,
		repoToken:   new(atomic.Value),
		host:        dcs.host,
		root:        dcs.root,
		csClient:    dcs.csClient,
		finalizer:   dcs.finalizer,
		cache:       dcs.cache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: fetcher,
		params:      dcs.params,
		stats:       dcs.stats,
	}
}

func (dcs *DoltChunkStore) WithNoopChunkCache() *DoltChunkStore {
	return &DoltChunkStore{
		repoId:      dcs.repoId,
		repoPath:    dcs.repoPath,
		repoToken:   new(atomic.Value),
		host:        dcs.host,
		root:        dcs.root,
		csClient:    dcs.csClient,
		finalizer:   dcs.finalizer,
		cache:       noopChunkCache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		params:      dcs.params,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) WithChunkCache(cache ChunkCache) *DoltChunkStore {
	return &DoltChunkStore{
		repoId:      dcs.repoId,
		repoPath:    dcs.repoPath,
		repoToken:   new(atomic.Value),
		host:        dcs.host,
		root:        dcs.root,
		csClient:    dcs.csClient,
		finalizer:   dcs.finalizer,
		cache:       cache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		params:      dcs.params,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) WithNetworkRequestParams(params NetworkRequestParams) *DoltChunkStore {
	return &DoltChunkStore{
		repoId:      dcs.repoId,
		repoPath:    dcs.repoPath,
		repoToken:   new(atomic.Value),
		host:        dcs.host,
		root:        dcs.root,
		csClient:    dcs.csClient,
		finalizer:   dcs.finalizer,
		cache:       dcs.cache,
		metadata:    dcs.metadata,
		nbf:         dcs.nbf,
		httpFetcher: dcs.httpFetcher,
		params:      params,
		stats:       dcs.stats,
		logger:      dcs.logger,
	}
}

func (dcs *DoltChunkStore) SetLogger(logger chunks.DebugLogger) {
	dcs.logger = logger
}

func (dcs *DoltChunkStore) SetFinalizer(f func() error) {
	dcs.finalizer = f
}

func (dcs *DoltChunkStore) logf(fmt string, args ...interface{}) {
	if dcs.logger != nil {
		dcs.logger.Logf(fmt, args...)
	}
}

func (dcs *DoltChunkStore) getRepoId() (*remotesapi.RepoId, string) {
	var token string
	curToken := dcs.repoToken.Load()
	if curToken != nil {
		token = curToken.(string)
	}
	return dcs.repoId, token
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

func (dcs *DoltChunkStore) ChunkFetcher(ctx context.Context) nbs.ChunkFetcher {
	return NewChunkFetcher(ctx, dcs)
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
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("decompressed_bytes", int64(decompressedSize)))
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
	ctx, span := tracer.Start(ctx, "remotestorage.GetManyCompressed")
	defer span.End()

	hashToChunk := dcs.cache.Get(hashes)

	span.SetAttributes(attribute.Int("num_hashes", len(hashes)), attribute.Int("cache_hits", len(hashToChunk)))
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
		err := dcs.readChunksAndCache(ctx, notCached, found)

		if err != nil {
			return err
		}
	}

	return nil
}

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

func (gr *GetRange) GetDownloadFunc(ctx context.Context, stats StatsRecorder, health reliable.HealthRecorder, fetcher HTTPFetcher, params NetworkRequestParams, chunkChan chan nbs.CompressedChunk, pathToUrl resourcePathToUrlFunc) func() error {
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
		rangeLen := gr.RangeLen()
		resp := reliable.StreamingRangeDownload(ctx, reliable.StreamingRangeRequest{
			Fetcher: fetcher,
			Offset:  gr.ChunkStartOffset(0),
			Length:  rangeLen,
			UrlFact: urlF,
			Stats:   stats,
			Health:  health,
			BackOffFact: func(ctx context.Context) backoff.BackOff {
				return downloadBackOff(ctx, params.DownloadRetryCount)
			},
			Throughput: reliable.MinimumThroughputCheck{
				CheckInterval: params.ThroughputMinimumCheckInterval,
				BytesPerCheck: params.ThroughputMinimumBytesPerCheck,
				NumIntervals:  params.ThroughputMinimumNumIntervals,
			},
			RespHeadersTimeout: params.RespHeadersTimeout,
		})
		defer resp.Close()
		reader := &RangeChunkReader{GetRange: gr, Reader: resp.Body}
		for {
			cc, err := reader.ReadChunk()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			select {
			case chunkChan <- cc:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	}
}

type RangeChunkReader struct {
	GetRange *GetRange
	Reader   io.Reader
	i        int
	skip     int
}

func (r *RangeChunkReader) ReadChunk() (nbs.CompressedChunk, error) {
	if r.skip > 0 {
		_, err := io.CopyN(io.Discard, r.Reader, int64(r.skip))
		if err != nil {
			return nbs.CompressedChunk{}, err
		}
		r.skip = 0
	}
	if r.i >= len(r.GetRange.Ranges) {
		return nbs.CompressedChunk{}, io.EOF
	}
	if r.i < len(r.GetRange.Ranges)-1 {
		r.skip = int(r.GetRange.GapBetween(r.i, r.i+1))
	}
	l := r.GetRange.Ranges[r.i].Length
	h := hash.New(r.GetRange.Ranges[r.i].Hash)
	r.i += 1
	buf := make([]byte, l)
	_, err := io.ReadFull(r.Reader, buf)
	if err != nil {
		return nbs.CompressedChunk{}, err
	} else {
		return nbs.NewCompressedChunk(h, buf)
	}
}

type locationRefresh struct {
	RefreshAfter   time.Time
	RefreshRequest *remotesapi.RefreshTableFileUrlRequest
	URL            string
	lastRefresh    time.Time
	mu             sync.Mutex
}

func (r *locationRefresh) Add(resp *remotesapi.DownloadLoc) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// TODO: These should be configurable in NetworkRequestParams or something.
var refreshTableFileURLRetryDuration = 5 * time.Second
var refreshTableFileURLTimeout = 15 * time.Second

func (r *locationRefresh) GetURL(ctx context.Context, lastError error, client remotesapi.ChunkStoreServiceClient) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.RefreshRequest != nil {
		now := time.Now()
		wantsRefresh := now.After(r.RefreshAfter) || errors.Is(lastError, HttpError)
		canRefresh := time.Since(r.lastRefresh) > refreshTableFileURLRetryDuration
		if wantsRefresh && canRefresh {
			ctx, cancel := context.WithTimeout(ctx, refreshTableFileURLTimeout)
			resp, err := client.RefreshTableFileUrl(ctx, r.RefreshRequest)
			cancel()
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

type RepoRequest interface {
	SetRepoId(*remotesapi.RepoId)
	SetRepoToken(string)
	SetRepoPath(string)
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes []hash.Hash, found func(context.Context, nbs.CompressedChunk)) (err error) {
	toSend := hash.NewHashSet(hashes...)

	fetcher := dcs.ChunkFetcher(ctx)
	defer func() {
		cerr := fetcher.Close()
		if err == nil {
			err = cerr
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := fetcher.Get(egCtx, toSend)
		if err != nil {
			return err
		}
		return fetcher.CloseSend()
	})
	eg.Go(func() error {
		for {
			cc, err := fetcher.Recv(egCtx)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			// Don't forward on empty/not found chunks.
			if len(cc.CompressedData) > 0 {
				if dcs.cache.PutChunk(cc) {
					return ErrCacheCapacityExceeded
				}
				found(egCtx, cc)
			}
		}
	})

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
		id, token := dcs.getRepoId()
		req := &remotesapi.HasChunksRequest{RepoId: id, RepoToken: token, Hashes: currByteSl, RepoPath: dcs.repoPath}
		var resp *remotesapi.HasChunksResponse
		resp, err = dcs.csClient.HasChunks(ctx, req)
		if err != nil {
			err = NewRpcError(err, "HasChunks", dcs.host, req)
			return true
		}

		if resp.RepoToken != "" {
			dcs.repoToken.Store(resp.RepoToken)
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
			return hash.HashSet{}, ErrCacheCapacityExceeded
		}
	}

	return absent, nil
}

func (dcs *DoltChunkStore) errorIfDangling(ctx context.Context, addrs hash.HashSet) error {
	absent, err := dcs.HasMany(ctx, addrs)
	if err != nil {
		return err
	}
	if len(absent) != 0 {
		s := absent.String()
		return fmt.Errorf("Found dangling references to %s", s)
	}
	return nil
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	addrs := hash.NewHashSet()
	err := getAddrs(c)(ctx, addrs, func(h hash.Hash) bool { return false })
	if err != nil {
		return err
	}
	err = dcs.errorIfDangling(ctx, addrs)
	if err != nil {
		return err
	}

	cc := nbs.ChunkToCompressedChunk(c)
	if dcs.cache.Put([]nbs.CompressedChunk{cc}) {
		return ErrCacheCapacityExceeded
	}
	return nil
}

// Returns the NomsBinFormat with which this ChunkSource is compatible.
func (dcs *DoltChunkStore) Version() string {
	return dcs.metadata.NbfVersion
}

func (dcs *DoltChunkStore) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (dcs *DoltChunkStore) Rebase(ctx context.Context) error {
	err := dcs.loadRoot(ctx)
	if err != nil {
		return err
	}
	return dcs.refreshRepoMetadata(ctx)
}

func (dcs *DoltChunkStore) refreshRepoMetadata(ctx context.Context) error {
	mdReq := &remotesapi.GetRepoMetadataRequest{
		RepoId:   dcs.repoId,
		RepoPath: dcs.repoPath,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}
	metadata, err := dcs.csClient.GetRepoMetadata(ctx, mdReq)
	if err != nil {
		return NewRpcError(err, "GetRepoMetadata", dcs.host, mdReq)
	}
	if metadata.RepoToken != "" {
		dcs.repoToken.Store(metadata.RepoToken)
	}
	dcs.metadata = metadata
	return nil
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (dcs *DoltChunkStore) Root(ctx context.Context) (hash.Hash, error) {
	return dcs.root, nil
}

func (dcs *DoltChunkStore) PushConcurrencyControl() chunks.PushConcurrencyControl {
	if dcs.metadata.PushConcurrencyControl == remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_ASSERT_WORKING_SET {
		return chunks.PushConcurrencyControl_AssertWorkingSet
	}

	if dcs.metadata.PushConcurrencyControl == remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED {
		if dcs.wsValidate {
			return chunks.PushConcurrencyControl_AssertWorkingSet
		}
	}

	return chunks.PushConcurrencyControl_IgnoreWorkingSet
}

func (dcs *DoltChunkStore) loadRoot(ctx context.Context) error {
	id, token := dcs.getRepoId()
	req := &remotesapi.RootRequest{RepoId: id, RepoToken: token, RepoPath: dcs.repoPath}
	resp, err := dcs.csClient.Root(ctx, req)
	if err != nil {
		return NewRpcError(err, "Root", dcs.host, req)
	}
	if resp.RepoToken != "" {
		dcs.repoToken.Store(resp.RepoToken)
	}
	dcs.root = hash.New(resp.RootHash)
	return nil
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

	id, _ := dcs.getRepoId()
	req := &remotesapi.CommitRequest{
		RepoId:         id,
		RepoPath:       dcs.repoPath,
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
	err = dcs.loadRoot(ctx)
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

func (dcs *DoltChunkStore) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	panic("runtime error: PersistGhostHashes should never be called on a remote chunk store")
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (dcs *DoltChunkStore) Close() error {
	return dcs.finalizer()
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
	hashToContentHash := make(map[hash.Hash][]byte)

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
		hashToContentHash[h] = md5Bytes[:]
	}

	for h, contentHash := range hashToContentHash {
		// Can parallelize this in the future if needed
		err := dcs.uploadTableFileWithRetries(ctx, h, uint64(hashToCount[h]), contentHash, func() (io.ReadCloser, uint64, error) {
			data := hashToData[h]
			return io.NopCloser(bytes.NewReader(data)), uint64(len(data)), nil
		})
		if err != nil {
			return map[hash.Hash]int{}, err
		}
	}

	return hashToCount, nil
}

func (dcs *DoltChunkStore) uploadTableFileWithRetries(ctx context.Context, tableFileId hash.Hash, numChunks uint64, tableFileContentHash []byte, getContent func() (io.ReadCloser, uint64, error)) error {
	op := func() error {
		body, contentLength, err := getContent()
		if err != nil {
			return err
		}

		tbfd := &remotesapi.TableFileDetails{
			Id:            tableFileId[:],
			ContentLength: contentLength,
			ContentHash:   tableFileContentHash,
			NumChunks:     numChunks,
		}

		dcs.logf("getting upload location for file %s", tableFileId.String())
		id, token := dcs.getRepoId()
		req := &remotesapi.GetUploadLocsRequest{RepoId: id, RepoToken: token, RepoPath: dcs.repoPath, TableFileDetails: []*remotesapi.TableFileDetails{tbfd}}
		resp, err := dcs.csClient.GetUploadLocations(ctx, req)
		if err != nil {
			err := NewRpcError(err, "GetUploadLocations", dcs.host, req)
			if err.IsPermanent() {
				return backoff.Permanent(err)
			}
			return err
		}

		if resp.RepoToken != "" {
			dcs.repoToken.Store(resp.RepoToken)
		}

		if len(resp.Locs) != 1 {
			return NewRpcError(errors.New("unexpected upload location count"), "GetUploadLocations", dcs.host, req)
		}
		loc := resp.Locs[0]

		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:

			// strip off the query parameters as they clutter the logs. We only
			// really care about being able to verify the table files are being
			// uploaded to the correct places on S3.
			urlStr := typedLoc.HttpPost.Url
			qmIdx := strings.IndexRune(urlStr, '?')
			if qmIdx != -1 {
				urlStr = urlStr[:qmIdx]
			}

			dcs.logf("uploading file %s to %s", tableFileId.String(), urlStr)
			err = dcs.httpPostUpload(ctx, typedLoc.HttpPost, tableFileContentHash, int64(contentLength), body)
			if err != nil {
				dcs.logf("failed to upload file %s to %s, err: %v", tableFileId.String(), urlStr, err)
				return err
			}
			dcs.logf("successfully uploaded file %s to %s", tableFileId.String(), urlStr)
		default:
			break
		}

		return nil
	}

	return backoff.Retry(op, uploadBackOff(ctx, dcs.params.UploadRetryCount))
}

type Sizer interface {
	Size() int64
}

func (dcs *DoltChunkStore) httpPostUpload(ctx context.Context, post *remotesapi.HttpPostTableFile, contentHash []byte, contentLength int64, body io.ReadCloser) error {
	return HttpPostUpload(ctx, dcs.httpFetcher, post, contentHash, contentLength, body)
}

func HttpPostUpload(ctx context.Context, httpFetcher HTTPFetcher, post *remotesapi.HttpPostTableFile, contentHash []byte, contentLength int64, body io.ReadCloser) error {
	fetcher := globalHttpFetcher
	if httpFetcher != nil {
		fetcher = httpFetcher
	}

	req, err := http.NewRequest(http.MethodPut, post.Url, body)
	if err != nil {
		return err
	}

	req.ContentLength = contentLength

	if len(contentHash) > 0 {
		md5s := base64.StdEncoding.EncodeToString(contentHash)
		req.Header.Set("Content-MD5", md5s)
	}

	resp, err := fetcher.Do(req.WithContext(ctx))

	if err == nil {
		defer func() {
			_ = resp.Body.Close()
		}()
	}

	return processHttpResp(resp, err)
}

const (
	chunkAggDistance = 8 * 1024
)

func (dcs *DoltChunkStore) SupportedOperations() chunks.TableFileStoreOps {
	return chunks.TableFileStoreOps{
		CanRead:  true,
		CanWrite: true,
		CanPrune: false,
		CanGC:    false,
	}
}

// WriteTableFile reads a table file from the provided reader and writes it to the chunk store.
func (dcs *DoltChunkStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	fileIdBytes := hash.Parse(fileId)
	err := dcs.uploadTableFileWithRetries(ctx, fileIdBytes, uint64(numChunks), contentHash, getRd)
	if err != nil {
		return err
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

	id, token := dcs.getRepoId()
	dcs.logf("Adding Table files to repo: %s -\n%s", dcs.repoPath, debugStr)
	atReq := &remotesapi.AddTableFilesRequest{
		RepoId:         id,
		RepoToken:      token,
		RepoPath:       dcs.repoPath,
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

	if atResp.RepoToken != "" {
		dcs.repoToken.Store(atResp.RepoToken)
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
func (dcs *DoltChunkStore) Sources(ctx context.Context) (hash.Hash, []chunks.TableFile, []chunks.TableFile, error) {
	id, token := dcs.getRepoId()
	req := &remotesapi.ListTableFilesRequest{RepoId: id, RepoPath: dcs.repoPath, RepoToken: token}
	resp, err := dcs.csClient.ListTableFiles(ctx, req)
	if err != nil {
		return hash.Hash{}, nil, nil, NewRpcError(err, "ListTableFiles", dcs.host, req)
	}
	if resp.RepoToken != "" {
		dcs.repoToken.Store(resp.RepoToken)
	}
	sourceFiles := getTableFiles(dcs, resp.TableFileInfo)
	appendixFiles := getTableFiles(dcs, resp.AppendixTableFileInfo)
	return hash.New(resp.RootHash), sourceFiles, appendixFiles, nil
}

func getTableFiles(dcs *DoltChunkStore, infoList []*remotesapi.TableFileInfo) []chunks.TableFile {
	tableFiles := make([]chunks.TableFile, 0)
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

// LocationPrefix
func (drtf DoltRemoteTableFile) LocationPrefix() string {
	return ""
}

// FileID gets the id of the file
func (drtf DoltRemoteTableFile) FileID() string {
	id := drtf.info.FileId

	// Early versions of |dolt| could return GenerationalChunkStore
	// TableFile implementations where FileID included an `oldgen/` prefix.
	// If we are communicating with a remotesrv from one of those versions,
	// we may see this prefix. This is not relevant to how we want to
	// address the table file locally, so we prune it here.
	if strings.HasPrefix(id, "oldgen/") {
		id = strings.TrimPrefix(id, "oldgen/")
	}

	return id
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
