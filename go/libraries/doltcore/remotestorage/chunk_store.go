package remotestorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/nbs"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
)

var ErrUploadFailed = errors.New("upload failed")

var globalHttpFetcher HTTPFetcher = &http.Client{}

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type DoltChunkStore struct {
	org         string
	repoName    string
	host        string
	csClient    remotesapi.ChunkStoreServiceClient
	cache       chunkCache
	httpFetcher HTTPFetcher
}

func NewDoltChunkStore(org, repoName, host string, csClient remotesapi.ChunkStoreServiceClient) *DoltChunkStore {
	return &DoltChunkStore{org, repoName, host, csClient, newMapChunkCache(), globalHttpFetcher}
}

func (dcs *DoltChunkStore) WithHTTPFetcher(fetcher HTTPFetcher) *DoltChunkStore {
	return &DoltChunkStore{dcs.org, dcs.repoName, dcs.host, dcs.csClient, dcs.cache, fetcher}
}

func (dcs *DoltChunkStore) getRepoId() *remotesapi.RepoId {
	return &remotesapi.RepoId{
		Org:      dcs.org,
		RepoName: dcs.repoName,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (dcs *DoltChunkStore) Get(ctx context.Context, h hash.Hash) chunks.Chunk {
	hashes := hash.HashSet{h: struct{}{}}
	foundChan := make(chan *chunks.Chunk, 1)
	dcs.GetMany(ctx, hashes, foundChan)

	select {
	case ch := <-foundChan:
		return *ch
	default:
		return chunks.EmptyChunk
	}
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (dcs *DoltChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
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
			//follow noms convention
			panic(err)
		}
	}
}

const (
	getLocsBatchSize      = 2048
	getLocsMaxConcurrency = 1
)

func (dcs *DoltChunkStore) getDLLocs(ctx context.Context, hashes []hash.Hash) (map[string][]*remotesapi.RangeChunk, error) {
	dlLocChan := make(chan *remotesapi.DownloadLoc, len(hashes))
	urlToRanges := make(map[string][]*remotesapi.RangeChunk)

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
					url := typedLoc.HttpGetRange.Url
					if ranges, ok := urlToRanges[url]; ok {
						urlToRanges[url] = append(ranges, typedLoc.HttpGetRange.Ranges...)
					} else {
						urlToRanges[url] = typedLoc.HttpGetRange.Ranges
					}
				}
			}
		}
	}()

	hashesBytes := HashesToSlices(hashes)
	var work []func() error
	batchItr(len(hashesBytes), getLocsBatchSize, func(st, end int) (stop bool) {
		batch := hashesBytes[st:end]
		f := func() error {
			req := remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), Hashes: batch}
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

	err := concurrentExec(work, getLocsMaxConcurrency)
	close(dlLocChan)

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return urlToRanges, nil
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes hash.HashSet, notCached []hash.Hash, foundChunks chan *chunks.Chunk) error {
	startGetLocs := time.Now()
	urlToRanges, err := dcs.getDLLocs(ctx, notCached)
	endGetLocs := time.Now()
	getLocsDelta := endGetLocs.Sub(startGetLocs)

	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	chunkChan := make(chan *chunks.Chunk)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range chunkChan {
			h := chunk.Hash()
			if !dcs.cache.PutChunk(chunk) {
				continue
			}

			if _, ok := hashes[h]; ok {
				foundChunks <- chunk
			}
		}
	}()

	startDL := time.Now()
	err = dcs.downloadChunks(ctx, urlToRanges, chunkChan)
	close(chunkChan)

	wg.Wait()

	if err != nil {
		return err
	}

	endDL := time.Now()
	dl := endDL.Sub(startDL)

	fmt.Sprintf("For %d chunks: Get Locs %f ms, Download %f ms\n", len(notCached), getLocsDelta.Seconds()*1000.0, dl.Seconds()*1000.0)

	return nil
}

// Returns true iff the value at the address |h| is contained in the
// store
func (dcs *DoltChunkStore) Has(ctx context.Context, h hash.Hash) bool {
	hashes := hash.HashSet{h: struct{}{}}
	absent := dcs.HasMany(ctx, hashes)

	return len(absent) == 0
}

const maxHasManyBatchSize = 16 * 1024

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (dcs *DoltChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet) {
	// get the set of hashes that isn't already in the cache
	notCached := dcs.cache.Has(hashes)

	if len(notCached) == 0 {
		return notCached
	}

	// convert the set to a slice of hashes and a corresponding slice of the byte encoding for those hashes
	hashSl, byteSl := HashSetToSlices(notCached)

	absent = make(hash.HashSet)
	var found []chunks.Chunk
	batchItr(len(hashSl), maxHasManyBatchSize, func(st, end int) (stop bool) {
		// slice the slices into a batch of hashes
		currHashSl := hashSl[st:end]
		currByteSl := byteSl[st:end]

		// send a request to the remote api to determine which chunks the remote api already has
		req := remotesapi.HasChunksRequest{RepoId: dcs.getRepoId(), Hashes: currByteSl}
		resp, err := dcs.csClient.HasChunks(ctx, &req)

		if err != nil {
			rpcErr := NewRpcError(err, "HasMany", dcs.host, req)
			//follow noms convention
			panic(rpcErr)
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

	if len(found)+len(absent) != len(notCached) {
		panic("not all chunks were accounted for")
	}

	if len(found) > 0 {
		dcs.cache.Put(found)
	}

	return absent
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(ctx context.Context, c chunks.Chunk) {
	dcs.cache.Put([]chunks.Chunk{c})
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (dcs *DoltChunkStore) Version() string {
	return constants.NomsVersion
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (dcs *DoltChunkStore) Rebase(ctx context.Context) {
	req := &remotesapi.RebaseRequest{RepoId: dcs.getRepoId()}
	_, err := dcs.csClient.Rebase(ctx, req)

	if err != nil {
		rpcErr := NewRpcError(err, "Rebase", dcs.host, req)

		// follow noms convention
		panic(rpcErr)
	}
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (dcs *DoltChunkStore) Root(ctx context.Context) hash.Hash {
	req := &remotesapi.RootRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.Root(ctx, req)

	if err != nil {
		rpcErr := NewRpcError(err, "Root", dcs.host, req)

		// follow noms convention
		panic(rpcErr)
	}

	return hash.New(resp.RootHash)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (dcs *DoltChunkStore) Commit(ctx context.Context, current, last hash.Hash) bool {
	hashToChunkCount, err := dcs.uploadChunks(ctx)

	if err != nil {
		// follow noms convention
		panic(err)
	}

	chnkTblInfo := make([]*remotesapi.ChunkTableInfo, 0, len(hashToChunkCount))
	for h, cnt := range hashToChunkCount {
		chnkTblInfo = append(chnkTblInfo, &remotesapi.ChunkTableInfo{Hash: h[:], ChunkCount: uint32(cnt)})
	}

	req := &remotesapi.CommitRequest{RepoId: dcs.getRepoId(), Current: current[:], Last: last[:], ChunkTableInfo: chnkTblInfo}
	resp, err := dcs.csClient.Commit(ctx, req)

	if err != nil {
		rpcErr := NewRpcError(err, "Commit", dcs.host, req)

		// follow noms convention
		panic(rpcErr)
	}

	return resp.Success
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

	req := &remotesapi.GetUploadLocsRequest{RepoId: dcs.getRepoId(), Hashes: hashBytes}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return map[hash.Hash]int{}, err
	}

	for _, loc := range resp.Locs {
		var err error
		h := hash.New(loc.Hash)
		data := hashToData[h]
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:
			err = dcs.httpPostUpload(ctx, loc.Hash, typedLoc.HttpPost, data)
		default:
			break
		}

		if err != nil {
			return map[hash.Hash]int{}, err
		}
	}

	return hashToCount, nil
}

func (dcs *DoltChunkStore) httpPostUpload(ctx context.Context, hashBytes []byte, post *remotesapi.HttpPostChunk, data []byte) error {
	//resp, err := http(post.Url, "application/octet-stream", bytes.NewBuffer(data))
	req, err := http.NewRequest(http.MethodPut, post.Url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	resp, err := dcs.httpFetcher.Do(req.WithContext(ctx))

	if err != nil {
		return err
	}

	if resp.StatusCode/100 != 2 {
		return ErrUploadFailed
	}

	return nil
}

func aggregateDownloads(aggDistance uint64, urlToRanges map[string][]*remotesapi.RangeChunk) []*remotesapi.DownloadLoc {
	var aggregatedLocs []*remotesapi.DownloadLoc
	for url, ranges := range urlToRanges {
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].Offset < ranges[j].Offset
		})

		last := ranges[0]
		aggregatedRanges := []*remotesapi.RangeChunk{last}
		for i := 1; i < len(ranges); i++ {
			curr := ranges[i]
			distance := last.Offset + uint64(last.Length) - curr.Offset - 1

			if distance <= aggDistance {
				aggregatedRanges = append(aggregatedRanges, curr)
			} else {
				getRange := &remotesapi.HttpGetRange{Url: url, Ranges: aggregatedRanges}
				aggregatedLocs = append(aggregatedLocs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})

				aggregatedRanges = []*remotesapi.RangeChunk{curr}
			}

			last = curr
		}

		getRange := &remotesapi.HttpGetRange{Url: url, Ranges: aggregatedRanges}
		aggregatedLocs = append(aggregatedLocs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
	}

	return aggregatedLocs
}

const (
	chunkAggDistance       = 0
	maxDownloadConcurrency = 1
)

// getting this working using the simplest approach first
func (dcs *DoltChunkStore) downloadChunks(ctx context.Context, urlToRanges map[string][]*remotesapi.RangeChunk, chunkChan chan *chunks.Chunk) error {
	var allChunks []chunks.Chunk
	aggLocs := aggregateDownloads(chunkAggDistance, urlToRanges)

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

	err := concurrentExec(work, maxDownloadConcurrency)

	return err
}

type bytesResult struct {
	r    *remotesapi.RangeChunk
	data []byte
	err  error
}

func (dcs *DoltChunkStore) getRangeDownloadFunc(ctx context.Context, url string, ranges []*remotesapi.RangeChunk, chunkChan chan *chunks.Chunk) func() error {
	numRanges := len(ranges)
	offset := ranges[0].Offset
	length := ranges[numRanges-1].Offset - offset + uint64(ranges[numRanges-1].Length)

	return func() error {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}

		rangeVal := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		req.Header.Set("Range", rangeVal)
		resp, err := dcs.httpFetcher.Do(req.WithContext(ctx))

		if err != nil {
			return err
		} else if resp.StatusCode/100 != 2 {
			return errors.New(url + " returned " + strconv.FormatInt(int64(resp.StatusCode), 10))
		}

		comprData, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return err
		}

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

func (dcs *DoltChunkStore) getDownloadWorkForLoc(ctx context.Context, getRange *remotesapi.HttpGetRange, chunkChan chan *chunks.Chunk) []func() error {
	var work []func() error

	rangeCount := len(getRange.Ranges)

	if rangeCount == 0 {
		return work
	}

	return []func() error{dcs.getRangeDownloadFunc(ctx, getRange.Url, getRange.Ranges, chunkChan)}
}
