package remotestorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/golang/snappy"
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

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
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

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
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
		chnks, err := dcs.readChunksAndCache(ctx, notCached)

		if err != nil {
			//follow noms convention
			panic(err)
		}

		for i := 0; i < len(chnks); i++ {
			c := chnks[i]
			foundChunks <- &c
		}
	}
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes []hash.Hash) ([]chunks.Chunk, error) {
	// read all from remote and cache and put in known
	hashesBytes := HashesToSlices(hashes)
	req := remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), Hashes: hashesBytes}
	resp, err := dcs.csClient.GetDownloadLocations(ctx, &req)

	if err != nil {
		return nil, NewRpcError(err, "GetDownloadLocations", dcs.host, req)
	}

	chnks, err := dcs.downloadChunks(ctx, resp.Locs)

	if err != nil {
		return nil, err
	}

	dcs.cache.Put(chnks)

	return chnks, nil
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
	for st, end := 0, maxHasManyBatchSize; st < len(hashSl); st, end = end, end+maxHasManyBatchSize {
		// slice the slices into a batch of hashes
		currHashSl := hashSl[st:]
		currByteSl := byteSl[st:]

		if end < len(hashSl) {
			currHashSl = hashSl[st:end]
			currByteSl = byteSl[st:end]
		}

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
	}

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

// getting this working using the simplest approach first
func (dcs *DoltChunkStore) downloadChunks(ctx context.Context, locs []*remotesapi.DownloadLoc) ([]chunks.Chunk, error) {
	var allChunks []chunks.Chunk

	for _, loc := range locs {
		var err error
		var chnks []chunks.Chunk
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.DownloadLoc_HttpGet:
			chnks, err = dcs.httpGetDownload(ctx, typedLoc.HttpGet)
		case *remotesapi.DownloadLoc_HttpGetRange:
			chnks, err = dcs.httpGetRangeDownload(ctx, typedLoc.HttpGetRange)
		}

		if err != nil {
			return allChunks, err
		}

		allChunks = append(allChunks, chnks...)
	}

	return allChunks, nil
}

func (dcs *DoltChunkStore) httpGetDownload(ctx context.Context, httpGet *remotesapi.HttpGetChunk) ([]chunks.Chunk, error) {
	hashes := httpGet.Hashes
	if len(hashes) != 1 {
		return nil, errors.New("not supported yet")
	}

	req, err := http.NewRequest(http.MethodGet, httpGet.Url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := dcs.httpFetcher.Do(req.WithContext(ctx))

	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	expectedHash := hash.New(hashes[0])
	ch := chunks.NewChunk(data)

	if ch.Hash() != expectedHash {
		return nil, errors.New("content did not match hash.")
	}

	return []chunks.Chunk{ch}, nil
}

type bytesResult struct {
	r    *remotesapi.RangeChunk
	data []byte
	err  error
}

func getRanges(ctx context.Context, httpFetcher HTTPFetcher, url string, rangeChan <-chan *remotesapi.RangeChunk, resultChan chan<- bytesResult, stopChan <-chan struct{}) {
	for {
		select {
		case <-stopChan:
			return
		default:
		}
		select {
		case r, ok := <-rangeChan:
			if !ok {
				return
			}

			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				resultChan <- bytesResult{r, nil, err}
				break
			}
			rangeVal := fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+uint64(r.Length)-1)
			req.Header.Set("Range", rangeVal)
			resp, err := httpFetcher.Do(req.WithContext(ctx))

			if err != nil {
				resultChan <- bytesResult{r, nil, err}
				break
			} else if resp.StatusCode/100 != 2 {
				resultChan <- bytesResult{r, nil, errors.New(url + " returned " + strconv.FormatInt(int64(resp.StatusCode), 10))}
				break
			}

			comprData, err := ioutil.ReadAll(resp.Body)

			if err != nil {
				resultChan <- bytesResult{r, nil, err}
				break
			}

			data, err := snappy.Decode(nil, comprData[:len(comprData)-4])

			if err != nil {
				resultChan <- bytesResult{r, nil, err}
				break
			}

			resultChan <- bytesResult{r, data, nil}

		case <-stopChan:
			return
		}
	}
}

func (dcs *DoltChunkStore) httpGetRangeDownload(ctx context.Context, getRange *remotesapi.HttpGetRange) ([]chunks.Chunk, error) {
	url := getRange.Url
	rangeCount := len(getRange.Ranges)

	if rangeCount == 0 {
		return []chunks.Chunk{}, nil
	}

	concurrency := rangeCount / 8

	if concurrency == 0 {
		concurrency = 1
	} else if concurrency > 128 {
		concurrency = 128
	}

	stopChan := make(chan struct{})
	rangeChan := make(chan *remotesapi.RangeChunk, len(getRange.Ranges))
	resultChan := make(chan bytesResult, 2*concurrency)

	for i := 0; i < concurrency; i++ {
		go getRanges(ctx, dcs.httpFetcher, url, rangeChan, resultChan, stopChan)
	}

	for _, r := range getRange.Ranges {
		rangeChan <- r
	}

	close(rangeChan)

	var chnks []chunks.Chunk
	for res := range resultChan {
		if res.err != nil {
			close(stopChan)
			return nil, res.err
		}

		r := res.r

		expectedHash := hash.New(r.Hash)
		ch := chunks.NewChunk(res.data)

		if ch.Hash() != expectedHash {
			close(stopChan)
			return nil, errors.New("content did not match hash.")
		}

		chnks = append(chnks, ch)

		if len(chnks) == rangeCount {
			break
		}
	}

	return chnks, nil
}
