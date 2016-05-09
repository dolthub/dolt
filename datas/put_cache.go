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
	"github.com/syndtr/goleveldb/leveldb/util"
)

func newUnwrittenPutCache() *unwrittenPutCache {
	dir, err := ioutil.TempDir("", "")
	d.Exp.NoError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: 24,
		WriteBuffer:            1 << 24, // 16MiB,
	})
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &unwrittenPutCache{
		orderedChunks: db,
		chunkIndex:    map[ref.Ref][]byte{},
		dbDir:         dir,
		mu:            &sync.Mutex{},
	}
}

type unwrittenPutCache struct {
	orderedChunks *leveldb.DB
	chunkIndex    map[ref.Ref][]byte
	dbDir         string
	mu            *sync.Mutex
	next          uint64
}

func (p *unwrittenPutCache) Add(c chunks.Chunk) bool {
	hash := c.Ref()
	dbKey, present := func() (dbKey []byte, present bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, present = p.chunkIndex[hash]; !present {
			dbKey = toDbKey(p.next)
			p.next++
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

func (p *unwrittenPutCache) Has(hash ref.Ref) (has bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, has = p.chunkIndex[hash]
	return
}

func (p *unwrittenPutCache) Get(hash ref.Ref) chunks.Chunk {
	// Don't use defer p.mu.Unlock() here, because I want reading from orderedChunks NOT to be guarded by the lock. LevelDB handles its own goroutine-safety.
	p.mu.Lock()
	dbKey, ok := p.chunkIndex[hash]
	p.mu.Unlock()

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

func (p *unwrittenPutCache) Clear(hashes ref.RefSlice) {
	deleteBatch := &leveldb.Batch{}
	p.mu.Lock()
	for _, hash := range hashes {
		deleteBatch.Delete(p.chunkIndex[hash])
		delete(p.chunkIndex, hash)
	}
	p.mu.Unlock()
	d.Chk.NoError(p.orderedChunks.Write(deleteBatch, nil))
	return
}

func toDbKey(idx uint64) []byte {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, idx)
	d.Chk.NoError(err)
	return buf.Bytes()
}

func fromDbKey(key []byte) (idx uint64) {
	err := binary.Read(bytes.NewReader(key), binary.BigEndian, &idx)
	d.Chk.NoError(err)
	return
}

func (p *unwrittenPutCache) ExtractChunks(start, end ref.Ref, w io.Writer) error {
	p.mu.Lock()
	iterRange := &util.Range{
		Start: p.chunkIndex[start],
		Limit: toDbKey(fromDbKey(p.chunkIndex[end]) + 1),
	}
	p.mu.Unlock()

	iter := p.orderedChunks.NewIterator(iterRange, nil)
	defer iter.Release()
	for iter.Next() {
		_, err := w.Write(iter.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *unwrittenPutCache) Destroy() error {
	d.Chk.NoError(p.orderedChunks.Close())
	return os.RemoveAll(p.dbDir)
}
