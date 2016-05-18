package datas

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func newOrderedChunkCache() *orderedChunkCache {
	dir, err := ioutil.TempDir("", "")
	d.Exp.NoError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: 24,
		WriteBuffer:            1 << 24, // 16MiB,
	})
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &orderedChunkCache{
		orderedChunks: db,
		chunkIndex:    map[ref.Ref][]byte{},
		dbDir:         dir,
		mu:            &sync.RWMutex{},
	}
}

// orderedChunkCache holds Chunks, allowing them to be retrieved by hash or enumerated in ref-height order.
type orderedChunkCache struct {
	orderedChunks *leveldb.DB
	chunkIndex    map[ref.Ref][]byte
	dbDir         string
	mu            *sync.RWMutex
}

type hashSet map[ref.Ref]struct{}

func (hs hashSet) Insert(hash ref.Ref) {
	hs[hash] = struct{}{}
}

func (hs hashSet) Has(hash ref.Ref) (has bool) {
	_, has = hs[hash]
	return
}

// Insert can be called from any goroutine to store c in the cache. If c is successfully added to the cache, Insert returns true. If c was already in the cache, Insert returns false.
func (p *orderedChunkCache) Insert(c chunks.Chunk, refHeight uint64) bool {
	hash := c.Ref()
	dbKey, present := func() (dbKey []byte, present bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, present = p.chunkIndex[hash]; !present {
			dbKey = toDbKey(refHeight, c.Ref())
			p.chunkIndex[hash] = dbKey
		}
		return
	}()

	if !present {
		buf := &bytes.Buffer{}
		sz := chunks.NewSerializer(buf)
		sz.Put(c)
		sz.Close()
		d.Chk.NoError(p.orderedChunks.Put(dbKey, buf.Bytes(), nil))
		return true
	}
	return false
}

func (p *orderedChunkCache) has(hash ref.Ref) (has bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, has = p.chunkIndex[hash]
	return
}

// Get can be called from any goroutine to retrieve the chunk referenced by hash. If the chunk is not present, Get returns the empty Chunk.
func (p *orderedChunkCache) Get(hash ref.Ref) chunks.Chunk {
	// Don't use defer p.mu.RUnlock() here, because I want reading from orderedChunks NOT to be guarded by the lock. LevelDB handles its own goroutine-safety.
	p.mu.RLock()
	dbKey, ok := p.chunkIndex[hash]
	p.mu.RUnlock()

	if !ok {
		return chunks.EmptyChunk
	}
	data, err := p.orderedChunks.Get(dbKey, nil)
	d.Chk.NoError(err)
	reader := bytes.NewReader(data)
	chunkChan := make(chan chunks.Chunk)
	go chunks.DeserializeToChan(reader, chunkChan)
	return <-chunkChan
}

// Clear can be called from any goroutine to remove chunks referenced by the given hashes from the cache.
func (p *orderedChunkCache) Clear(hashes hashSet) {
	deleteBatch := &leveldb.Batch{}
	p.mu.Lock()
	for hash := range hashes {
		deleteBatch.Delete(p.chunkIndex[hash])
		delete(p.chunkIndex, hash)
	}
	p.mu.Unlock()
	d.Chk.NoError(p.orderedChunks.Write(deleteBatch, nil))
	return
}

var uint64Size = binary.Size(uint64(0))

// toDbKey takes a refHeight and a hash and returns a binary key suitable for use with LevelDB. The default sort order used by LevelDB ensures that these keys (and their associated values) will be iterated in ref-height order.
func toDbKey(refHeight uint64, hash ref.Ref) []byte {
	digest := hash.DigestSlice()
	buf := bytes.NewBuffer(make([]byte, 0, uint64Size+binary.Size(digest)))
	err := binary.Write(buf, binary.BigEndian, refHeight)
	d.Chk.NoError(err)
	err = binary.Write(buf, binary.BigEndian, digest)
	d.Chk.NoError(err)
	return buf.Bytes()
}

func fromDbKey(key []byte) (uint64, ref.Ref) {
	refHeight := uint64(0)
	r := bytes.NewReader(key)
	err := binary.Read(r, binary.BigEndian, &refHeight)
	d.Chk.NoError(err)
	digest := ref.Sha1Digest{}
	err = binary.Read(r, binary.BigEndian, &digest)
	d.Chk.NoError(err)
	return refHeight, ref.New(digest)
}

// ExtractChunks can be called from any goroutine to write Chunks referenced by the given hashes to w. The chunks are ordered by ref-height. Chunks of the same height are written in an unspecified order, relative to one another.
func (p *orderedChunkCache) ExtractChunks(hashes hashSet, w io.Writer) error {
	iter := p.orderedChunks.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		_, hash := fromDbKey(iter.Key())
		if !hashes.Has(hash) {
			continue
		}
		_, err := w.Write(iter.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *orderedChunkCache) Destroy() error {
	d.Chk.NoError(p.orderedChunks.Close())
	return os.RemoveAll(p.dbDir)
}
