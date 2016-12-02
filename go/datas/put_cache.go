// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/golang/snappy"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func newOrderedChunkCache() *orderedChunkCache {
	dir, err := ioutil.TempDir("", "")
	d.PanicIfError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: 24,
		NoSync:                 true,    // We dont need this data to be durable. LDB is acting as sorting temporary storage that can be larger than main memory.
		WriteBuffer:            1 << 27, // 128MiB
	})
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &orderedChunkCache{
		orderedChunks: db,
		chunkIndex:    map[hash.Hash][]byte{},
		dbDir:         dir,
		mu:            &sync.RWMutex{},
	}
}

// orderedChunkCache holds Chunks, allowing them to be retrieved by hash or enumerated in ref-height order.
type orderedChunkCache struct {
	orderedChunks *leveldb.DB
	chunkIndex    map[hash.Hash][]byte
	dbDir         string
	mu            *sync.RWMutex
}

// Insert can be called from any goroutine to store c in the cache. If c is successfully added to the cache, Insert returns true. If c was already in the cache, Insert returns false.
func (p *orderedChunkCache) Insert(c chunks.Chunk, refHeight uint64) bool {
	hash := c.Hash()
	dbKey, present := func() (dbKey []byte, present bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, present = p.chunkIndex[hash]; !present {
			dbKey = toDbKey(refHeight, c.Hash())
			p.chunkIndex[hash] = dbKey
		}
		return
	}()

	if !present {
		compressed := snappy.Encode(nil, c.Data())
		d.Chk.NoError(p.orderedChunks.Put(dbKey, compressed, nil))
		return true
	}
	return false
}

func (p *orderedChunkCache) has(hash hash.Hash) (has bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, has = p.chunkIndex[hash]
	return
}

// Get can be called from any goroutine to retrieve the chunk referenced by hash. If the chunk is not present, Get returns the empty Chunk.
func (p *orderedChunkCache) Get(hash hash.Hash) chunks.Chunk {
	// Don't use defer p.mu.RUnlock() here, because I want reading from orderedChunks NOT to be guarded by the lock. LevelDB handles its own goroutine-safety.
	p.mu.RLock()
	dbKey, ok := p.chunkIndex[hash]
	p.mu.RUnlock()

	if !ok {
		return chunks.EmptyChunk
	}
	compressed, err := p.orderedChunks.Get(dbKey, nil)
	d.Chk.NoError(err)
	data, err := snappy.Decode(nil, compressed)
	d.Chk.NoError(err)
	return chunks.NewChunkWithHash(hash, data)
}

// Clear can be called from any goroutine to remove chunks referenced by the given hashes from the cache.
func (p *orderedChunkCache) Clear(hashes hash.HashSet) {
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
func toDbKey(refHeight uint64, h hash.Hash) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, uint64Size+hash.ByteLen))
	err := binary.Write(buf, binary.BigEndian, refHeight)
	d.Chk.NoError(err)
	err = binary.Write(buf, binary.BigEndian, h[:])
	d.Chk.NoError(err)
	return buf.Bytes()
}

func fromDbKey(key []byte) (uint64, hash.Hash) {
	refHeight := uint64(0)
	r := bytes.NewReader(key)
	err := binary.Read(r, binary.BigEndian, &refHeight)
	d.Chk.NoError(err)
	h := hash.Hash{}
	err = binary.Read(r, binary.BigEndian, &h)
	d.Chk.NoError(err)
	return refHeight, h
}

// ExtractChunks can be called from any goroutine to write Chunks referenced by the given hashes to w. The chunks are ordered by ref-height. Chunks of the same height are written in an unspecified order, relative to one another.
func (p *orderedChunkCache) ExtractChunks(hashes hash.HashSet, chunkChan chan *chunks.Chunk) error {
	iter := p.orderedChunks.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		_, hash := fromDbKey(iter.Key())
		if !hashes.Has(hash) {
			continue
		}
		compressed := iter.Value()
		data, err := snappy.Decode(nil, compressed)
		d.Chk.NoError(err)
		c := chunks.NewChunkWithHash(hash, data)
		chunkChan <- &c
	}
	return nil
}

func (p *orderedChunkCache) Destroy() error {
	d.Chk.NoError(p.orderedChunks.Close())
	return os.RemoveAll(p.dbDir)
}
