package remotestorage

import (
	"bytes"
	"context"
	"errors"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"io/ioutil"
	"net/http"
	"sort"
)

type DoltChunkStore struct {
	org      string
	repoName string
	csClient remotesapi.ChunkStoreServiceClient
	cache    chunkCache
}

func NewDoltChunkStore(org, repoName string, csClient remotesapi.ChunkStoreServiceClient) *DoltChunkStore {
	return &DoltChunkStore{org, repoName, csClient, newMapChunkCache()}
}

func (dcs *DoltChunkStore) getRepoId() *remotesapi.RepoId {
	return &remotesapi.RepoId{
		Org:      dcs.org,
		RepoName: dcs.repoName,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
func (dcs *DoltChunkStore) Get(h hash.Hash) chunks.Chunk {
	hashes := hash.HashSet{h: struct{}{}}
	foundChan := make(chan *chunks.Chunk, 1)
	dcs.GetMany(hashes, foundChan)

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
func (dcs *DoltChunkStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
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

	chnks, err := dcs.readChunksAndCache(notCached)

	if err != nil {
		//follow noms convention
		panic(err)
	}

	for i := 0; i < len(chnks); i++ {
		c := chnks[i]
		foundChunks <- &c
	}
}

func (dcs *DoltChunkStore) readChunksAndCache(hashes []hash.Hash) ([]chunks.Chunk, error) {
	// read all from remote and cache and put in known
	hashesBytes := HashesToSlices(hashes)
	req := remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), Hashes: hashesBytes}
	resp, err := dcs.csClient.GetDownloadLocations(context.Background(), &req)

	if err != nil {
		return nil, err
	}

	chnks, err := dcs.downloadChunks(resp.Locs)

	if err != nil {
		return nil, err
	}

	dcs.cache.Put(chnks)

	return chnks, nil
}

// Returns true iff the value at the address |h| is contained in the
// store
func (dcs *DoltChunkStore) Has(h hash.Hash) bool {
	hashes := hash.HashSet{h: struct{}{}}
	absent := dcs.HasMany(hashes)

	return len(absent) == 0
}

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (dcs *DoltChunkStore) HasMany(hashes hash.HashSet) (absent hash.HashSet) {
	notCached := dcs.cache.Has(hashes)

	if len(notCached) == 0 {
		return notCached
	}

	hashSl, byteSl := HashSetToSlices(notCached)
	req := remotesapi.HasChunksRequest{RepoId: dcs.getRepoId(), Hashes: byteSl}
	resp, err := dcs.csClient.HasChunks(context.Background(), &req)

	if err != nil {
		//follow noms convention
		panic(err)
	}

	numAbsent := len(resp.Absent)
	sort.Slice(resp.Absent, func(i, j int) bool {
		return resp.Absent[i] < resp.Absent[j]
	})

	absent = make(hash.HashSet)
	found := make([]chunks.Chunk, 0, len(notCached)-numAbsent)

	for i, j := 0, 0; i < len(hashSl); i++ {
		currHash := hashSl[i]

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

	if len(found) > 0 {
		dcs.cache.Put(found)
	}

	return absent
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(c chunks.Chunk) {
	dcs.cache.Put([]chunks.Chunk{c})
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (dcs *DoltChunkStore) Version() string {
	return constants.NomsVersion
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (dcs *DoltChunkStore) Rebase() {
	req := &remotesapi.RebaseRequest{RepoId: dcs.getRepoId()}
	_, err := dcs.csClient.Rebase(context.Background(), req)

	if err != nil {
		// follow noms convention
		panic(err)
	}
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (dcs *DoltChunkStore) Root() hash.Hash {
	req := &remotesapi.RootRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.Root(context.Background(), req)

	if err != nil {
		// follow noms convention
		panic(err)
	}

	return hash.New(resp.RootHash)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (dcs *DoltChunkStore) Commit(current, last hash.Hash) bool {
	err := dcs.uploadChunks()

	if err != nil {
		// follow noms convention
		panic(err)
	}

	req := &remotesapi.CommitRequest{RepoId: dcs.getRepoId(), Current: current[:], Last: last[:]}
	resp, err := dcs.csClient.Commit(context.Background(), req)

	if err != nil {
		// follow noms convention
		panic(err)
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
func (dcs *DoltChunkStore) uploadChunks() error {
	hashToChunk := dcs.cache.GetAndClearChunksToFlush()

	if len(hashToChunk) == 0 {
		return nil
	}

	i := 0
	hashBytes := make([][]byte, len(hashToChunk))
	for h := range hashToChunk {
		tmp := h
		hashBytes[i] = tmp[:]
		i++
	}

	ctx := context.Background()
	req := &remotesapi.GetUploadLocsRequest{RepoId: dcs.getRepoId(), Hashes: hashBytes}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return err
	}

	for _, loc := range resp.Locs {
		var err error
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:
			err = dcs.httpPostUpload(loc.Hashes, typedLoc.HttpPost, hashToChunk)
		default:
			break
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (dcs *DoltChunkStore) httpPostUpload(hashes [][]byte, post *remotesapi.HttpPostChunk, hashToChunk map[hash.Hash]chunks.Chunk) error {
	for _, hashBytes := range hashes {
		h := hash.New(hashBytes)
		if ch, ok := hashToChunk[h]; ok {
			data := ch.Data()
			http.Post(post.Url, "application/octet-stream", bytes.NewBuffer(data))
		} else {
			return errors.New("unknown chunk " + h.String())
		}
	}

	return nil
}

// getting this working using the simplest approach first
func (dcs *DoltChunkStore) downloadChunks(locs []*remotesapi.DownloadLoc) ([]chunks.Chunk, error) {
	var allChunks []chunks.Chunk

	for _, loc := range locs {
		var err error
		var chnks []chunks.Chunk
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.DownloadLoc_HttpGet:
			chnks, err = dcs.httpGetDownload(loc.Hashes, typedLoc.HttpGet)
		}

		if err != nil {
			return allChunks, err
		}

		allChunks = append(allChunks, chnks...)
	}

	return allChunks, nil
}

func (dcs *DoltChunkStore) httpGetDownload(hashes [][]byte, get *remotesapi.HttpGetChunk) ([]chunks.Chunk, error) {
	if len(hashes) != 1 {
		return nil, errors.New("not supported yet")
	}

	resp, err := http.Get(get.Url)

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
