// Copyright 2019 Liquidata, Inc.
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/golang/snappy"

	remotesapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrUploadFailed = errors.New("upload failed")
var ErrInvalidDoltSpecPath = errors.New("invalid dolt spec path")

var globalHttpFetcher HTTPFetcher = &http.Client{}

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

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type DoltChunkStore struct {
	org         string
	repoName    string
	host        string
	csClient    remotesapi.ChunkStoreServiceClient
	cache       chunkCache
	metadata    *remotesapi.GetRepoMetadataResponse
	nbf         *types.NomsBinFormat
	httpFetcher HTTPFetcher
}

func NewDoltChunkStoreFromPath(ctx context.Context, nbf *types.NomsBinFormat, path, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	tokens := strings.Split(strings.Trim(path, "/"), "/")
	if len(tokens) != 2 {
		return nil, ErrInvalidDoltSpecPath
	}

	// this may just be a dolthub thing.  Need to revisit how we do this.
	org := tokens[0]
	repoName := tokens[1]

	if _, ok := csClient.(RetryingChunkStoreServiceClient); !ok {
		csClient = RetryingChunkStoreServiceClient{csClient}
	}

	return NewDoltChunkStore(ctx, nbf, org, repoName, host, RetryingChunkStoreServiceClient{csClient})
}

func NewDoltChunkStore(ctx context.Context, nbf *types.NomsBinFormat, org, repoName, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	if _, ok := csClient.(RetryingChunkStoreServiceClient); !ok {
		csClient = RetryingChunkStoreServiceClient{csClient}
	}

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
	return &DoltChunkStore{org, repoName, host, csClient, newMapChunkCache(), metadata, nbf, globalHttpFetcher}, nil
}

func (dcs *DoltChunkStore) WithHTTPFetcher(fetcher HTTPFetcher) *DoltChunkStore {
	return &DoltChunkStore{dcs.org, dcs.repoName, dcs.host, dcs.csClient, dcs.cache, dcs.metadata, dcs.nbf, fetcher}
}

func (dcs *DoltChunkStore) WithNoopChunkCache() *DoltChunkStore {
	return &DoltChunkStore{dcs.org, dcs.repoName, dcs.host, dcs.csClient, noopChunkCache, dcs.metadata, dcs.nbf, dcs.httpFetcher}
}

func (dcs *DoltChunkStore) getRepoId() *remotesapi.RepoId {
	return &remotesapi.RepoId{
		Org:      dcs.org,
		RepoName: dcs.repoName,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (dcs *DoltChunkStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	hashes := hash.HashSet{h: struct{}{}}
	foundChan := make(chan *chunks.Chunk, 1)
	err := dcs.GetMany(ctx, hashes, foundChan)

	if err != nil {
		return chunks.EmptyChunk, err
	}

	select {
	case ch := <-foundChan:
		return *ch, nil
	default:
		return chunks.EmptyChunk, nil
	}
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (dcs *DoltChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *chunks.Chunk) error {
	hashToChunk := dcs.cache.Get(hashes)

	notCached := make([]hash.Hash, 0, len(hashes))
	for h := range hashes {
		c := hashToChunk[h]

		if c.IsEmpty() {
			notCached = append(notCached, h)
		} else {
			foundChunks <- &c
		}
	}

	if len(notCached) > 0 {
		err := dcs.readChunksAndCache(ctx, hashes, notCached, foundChunks)

		if err != nil {
			return err
		}
	}

	return nil
}

const (
	getLocsBatchSize      = 4096
	getLocsMaxConcurrency = 32
)

type urlAndRanges struct {
	Url    string
	Ranges []*remotesapi.RangeChunk
}

func (dcs *DoltChunkStore) getDLLocs(ctx context.Context, hashes []hash.Hash) (map[string]urlAndRanges, error) {
	// results aggregated in resourceToUrlAndRanges
	resourceToUrlAndRanges := make(map[string]urlAndRanges)

	// channel for receiving results from go routines making grpc calls to get download locations for chunks
	dlLocChan := make(chan *remotesapi.DownloadLoc, len(hashes))

	// go routine for receiving the results of the grpc calls and aggregating the results into resourceToUrlAndRanges
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for loc := range dlLocChan {
			switch typedLoc := loc.Location.(type) {
			case *remotesapi.DownloadLoc_HttpGet:
				panic("deprecated")
			case *remotesapi.DownloadLoc_HttpGetRange:
				if len(typedLoc.HttpGetRange.Ranges) > 0 {
					urlStr := typedLoc.HttpGetRange.Url
					urlObj, _ := url.Parse(urlStr)

					resourcePath := fmt.Sprintf("%s://%s%s", urlObj.Scheme, urlObj.Host, urlObj.Path)

					if uAndR, ok := resourceToUrlAndRanges[resourcePath]; ok {
						uAndR.Ranges = append(uAndR.Ranges, typedLoc.HttpGetRange.Ranges...)
						resourceToUrlAndRanges[resourcePath] = uAndR
					} else {
						resourceToUrlAndRanges[resourcePath] = urlAndRanges{urlStr, typedLoc.HttpGetRange.Ranges}
					}
				}
			}
		}
	}()

	hashesBytes := HashesToSlices(hashes)
	var work []func() error

	// batchItr creates work functions which request a batch of chunk download locations and write the results to the
	// dlLocChan
	batchItr(len(hashesBytes), getLocsBatchSize, func(st, end int) (stop bool) {
		batch := hashesBytes[st:end]
		f := func() error {
			req := remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), ChunkHashes: batch}
			resp, err := dcs.csClient.GetDownloadLocations(ctx, &req)

			if err != nil {
				return NewRpcError(err, "GetDownloadLocations", dcs.host, req)
			}

			for _, loc := range resp.Locs {
				dlLocChan <- loc
			}

			return nil
		}

		work = append(work, f)
		return false
	})

	var err error

	// execute the work and close the channel after as no more results will come in
	func() {
		defer close(dlLocChan)
		err = concurrentExec(work, getLocsMaxConcurrency)
	}()

	// wait for the result aggregator go routine to exit
	wg.Wait()

	if err != nil {
		return nil, err
	}

	return resourceToUrlAndRanges, nil
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes hash.HashSet, notCached []hash.Hash, foundChunks chan *chunks.Chunk) error {
	// get the locations where the chunks can be downloaded from
	resourceToUrlAndRanges, err := dcs.getDLLocs(ctx, notCached)

	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// channel to receive chunks on
	chunkChan := make(chan *chunks.Chunk, 128)

	// start a go routine to receive the downloaded chunks on
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range chunkChan {
			if !dcs.cache.PutChunk(chunk) {
				continue
			}

			h := chunk.Hash()
			if _, ok := hashes[h]; ok {
				foundChunks <- chunk
			}
		}
	}()

	// download the chunks and close the channel after
	func() {
		defer close(chunkChan)
		err = dcs.downloadChunks(ctx, resourceToUrlAndRanges, chunkChan)
	}()

	// wait for all the results to finish processing
	wg.Wait()

	if err != nil {
		return err
	}

	return nil
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
	var found []chunks.Chunk
	var err error

	batchItr(len(hashSl), maxHasManyBatchSize, func(st, end int) (stop bool) {
		// slice the slices into a batch of hashes
		currHashSl := hashSl[st:end]
		currByteSl := byteSl[st:end]

		// send a request to the remote api to determine which chunks the remote api already has
		req := remotesapi.HasChunksRequest{RepoId: dcs.getRepoId(), Hashes: currByteSl}
		resp, err := dcs.csClient.HasChunks(ctx, &req)

		if err != nil {
			err = NewRpcError(err, "HasMany", dcs.host, req)
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
				c := chunks.NewChunkWithHash(currHash, []byte{})
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
		dcs.cache.Put(found)
	}

	return absent, nil
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	dcs.cache.Put([]chunks.Chunk{c})
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

	return resp.Success, nil
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (dcs *DoltChunkStore) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (dcs *DoltChunkStore) StatsSummary() string {
	return "Unsupported"
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
	for _, ch := range hashToChunk {
		chnks = append(chnks, ch)
	}

	hashToCount := make(map[hash.Hash]int)
	hashToData := make(map[hash.Hash][]byte)
	// structuring so this can be done as multiple files in the future.
	{
		name, data, err := nbs.WriteChunks(chnks)

		if err != nil {
			return map[hash.Hash]int{}, err
		}

		h := hash.Parse(name)
		hashToData[h] = data
		hashToCount[h] = len(chnks)
	}

	hashBytes := make([][]byte, 0, len(hashToChunk))
	for h := range hashToData {
		tmp := h
		hashBytes = append(hashBytes, tmp[:])
	}

	req := &remotesapi.GetUploadLocsRequest{RepoId: dcs.getRepoId(), TableFileHashes: hashBytes}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return map[hash.Hash]int{}, err
	}

	for _, loc := range resp.Locs {
		var err error
		h := hash.New(loc.TableFileHash)
		data := hashToData[h]
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:
			err = dcs.httpPostUpload(ctx, loc.TableFileHash, typedLoc.HttpPost, data)
		default:
			break
		}

		if err != nil {
			return map[hash.Hash]int{}, err
		}
	}

	return hashToCount, nil
}

func (dcs *DoltChunkStore) httpPostUpload(ctx context.Context, hashBytes []byte, post *remotesapi.HttpPostTableFile, data []byte) error {
	//resp, err := http(post.Url, "application/octet-stream", bytes.NewBuffer(data))
	req, err := http.NewRequest(http.MethodPut, post.Url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	var resp *http.Response
	op := func() error {
		var err error
		resp, err = dcs.httpFetcher.Do(req.WithContext(ctx))
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
func aggregateDownloads(aggDistance uint64, resourceToUrlAndRanges map[string]urlAndRanges) []*remotesapi.DownloadLoc {
	// results are aggregated into here, and each element will represent a request to be made
	var aggregatedLocs []*remotesapi.DownloadLoc

	// for each file that we need to download chunks from
	for _, urlAndRanges := range resourceToUrlAndRanges {
		urlStr := urlAndRanges.Url
		ranges := urlAndRanges.Ranges

		// sort the ranges we need to get by the starting offset
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].Offset < ranges[j].Offset
		})

		// for each range in the sorted list of ranges group items that are less than aggDistance apart
		last := ranges[0]
		aggregatedRanges := []*remotesapi.RangeChunk{last}
		for i := 1; i < len(ranges); i++ {
			curr := ranges[i]
			distance := last.Offset + uint64(last.Length) - curr.Offset

			if distance <= aggDistance {
				// When close enough together aggregate
				aggregatedRanges = append(aggregatedRanges, curr)
			} else {
				// When not close enough together add a DownloadLoc encompassing all the aggregated chunks
				getRange := &remotesapi.HttpGetRange{Url: urlStr, Ranges: aggregatedRanges}
				aggregatedLocs = append(aggregatedLocs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})

				// start a new aggregation of ranges
				aggregatedRanges = []*remotesapi.RangeChunk{curr}
			}

			last = curr
		}

		// add the last DownloadLoc
		getRange := &remotesapi.HttpGetRange{Url: urlStr, Ranges: aggregatedRanges}
		aggregatedLocs = append(aggregatedLocs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
	}

	return aggregatedLocs
}

const (
	chunkAggDistance       = 8 * 1024
	maxDownloadConcurrency = 64
)

// creates work functions for each download and executes them in parallel.  The work functions write downloaded chunks
// to chunkChan
func (dcs *DoltChunkStore) downloadChunks(ctx context.Context, resourceToUrlAndRanges map[string]urlAndRanges, chunkChan chan *chunks.Chunk) error {
	var allChunks []chunks.Chunk
	aggLocs := aggregateDownloads(chunkAggDistance, resourceToUrlAndRanges)

	// loop over all the aggLocs that need to be downloaded and create a work function for each
	var work []func() error
	for _, loc := range aggLocs {
		var err error
		var chnks []chunks.Chunk
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.DownloadLoc_HttpGet:
			panic("deprecated")
			//chnks, err = dcs.httpGetDownload(ctx, typedLoc.HttpGet, foundChunks)
		case *remotesapi.DownloadLoc_HttpGetRange:
			downloadWork := dcs.getDownloadWorkForLoc(ctx, typedLoc.HttpGetRange, chunkChan)
			work = append(work, downloadWork...)
		}

		if err != nil {
			return err
		}

		allChunks = append(allChunks, chnks...)
	}

	// execute the work
	err := concurrentExec(work, maxDownloadConcurrency)

	return err
}

// getRangeDownloadFunc returns a work function that does the downloading of one or more chunks and writes those chunks
// to the chunkChan
func (dcs *DoltChunkStore) getRangeDownloadFunc(ctx context.Context, urlStr string, ranges []*remotesapi.RangeChunk, chunkChan chan *chunks.Chunk) func() error {
	numRanges := len(ranges)
	offset := ranges[0].Offset
	length := ranges[numRanges-1].Offset - offset + uint64(ranges[numRanges-1].Length)

	return func() error {
		comprData, err := rangeDownloadWithRetries(ctx, dcs.httpFetcher, offset, length, urlStr)

		if err != nil {
			return err
		}

		// loop over the ranges of bytes and extract those bytes from the data that was downloaded.  The extracted bytes
		// are then decoded to chunks and written to the chunkChan
		for _, r := range ranges {
			chunkStart := r.Offset - offset
			chunkEnd := chunkStart + uint64(r.Length) - 4
			chunkBytes, err := snappy.Decode(nil, comprData[chunkStart:chunkEnd])

			if err != nil {
				return err
			}

			chunk := chunks.NewChunk(chunkBytes)
			chunkChan <- &chunk
		}

		return nil
	}
}

// rangeDownloadWithRetries executes an http get with the 'Range' header to get a range of bytes from a file.  Request
// is executed with retries and if progress was made, downloads will be resumed from where they left off on subsequent attempts.
func rangeDownloadWithRetries(ctx context.Context, fetcher HTTPFetcher, offset, length uint64, urlStr string) ([]byte, error) {
	// create the request
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}

	// parameters used for resuming downloads.
	var allBufs [][]byte
	currOffset := offset
	currLength := length

	callNumber := -1
	//execute the request
	op := func() error {
		callNumber++
		rangeVal := fmt.Sprintf("bytes=%d-%d", currOffset, currOffset+currLength-1)
		req.Header.Set("Range", rangeVal)

		var resp *http.Response
		resp, err = fetcher.Do(req.WithContext(ctx))
		respErr := processHttpResp(resp, err)

		if respErr != nil {
			return respErr
		}

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

	err = backoff.Retry(op, backoff.WithMaxRetries(downRetryParams, downRetryCount))

	if err != nil {
		return nil, err
	}

	return collapseBuffers(allBufs, length), nil
}

func collapseBuffers(allBufs [][]byte, length uint64) []byte {
	collapsed := allBufs[0]
	if len(allBufs) > 1 {
		collapsed = make([]byte, length)

		pos := 0
		for i := 0; i < len(allBufs); i++ {
			copy(collapsed[pos:], allBufs[i])
			pos += len(allBufs[i])
		}
	}
	return collapsed
}

func (dcs *DoltChunkStore) getDownloadWorkForLoc(ctx context.Context, getRange *remotesapi.HttpGetRange, chunkChan chan *chunks.Chunk) []func() error {
	var work []func() error

	rangeCount := len(getRange.Ranges)

	if rangeCount == 0 {
		return work
	}

	return []func() error{dcs.getRangeDownloadFunc(ctx, getRange.Url, getRange.Ranges, chunkChan)}
}

// NewSink still needs to be implemented in order to write to a DoltChunkStore using the TableFileStore interface
func (dcs *DoltChunkStore) NewSink(ctx context.Context, fileId string, numChunks int) (nbs.WriteCloserWithContext, error) {
	panic("Not implemented")
}

// Sources retrieves the current root hash, and a list of all the table files
func (dcs *DoltChunkStore) Sources(ctx context.Context) (hash.Hash, []nbs.TableFile, error) {
	req := &remotesapi.EnumerateTablesRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.EnumerateTables(ctx, req)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	var tblFiles []nbs.TableFile
	for _, nfo := range resp.TableInfo {
		tblFiles = append(tblFiles, DoltRemoteTableFile{dcs, nfo})
	}

	return hash.New(resp.RootHash), tblFiles, nil
}

// SetRootChunk sets the root chunk changes the root chunk hash from the previous value to the new root.
func (dcs *DoltChunkStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	panic("Not Implemented")
}

// DoltRemoteTableFile is an implementation of a TableFile that live in a DoltChunkStore
type DoltRemoteTableFile struct {
	dcs  *DoltChunkStore
	info *remotesapi.TableInfo
}

// FileID gets the id of the file
func (drtf DoltRemoteTableFile) FileID() string {
	return drtf.info.FileId
}

// NumChunks returns the number of chunks in a table file
func (drtf DoltRemoteTableFile) NumChunks() int {
	return int(drtf.info.NumChunks)
}

// Open returns an io.ReadCloser which can be used to read the bytes of a table file.
func (drtf DoltRemoteTableFile) Open() (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, drtf.info.Url, nil)

	if err != nil {
		return nil, err
	}

	resp, err := drtf.dcs.httpFetcher.Do(req)

	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}
