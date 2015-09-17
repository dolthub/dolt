package chunks

import (
	"bytes"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var rootKey = []byte("/root")
var chunkPrefix = []byte("/chunk/")

func toChunkKey(r ref.Ref) []byte {
	digest := r.Digest()
	return append(chunkPrefix, digest[:]...)
}

type LevelDBStore struct {
	db       *leveldb.DB
	mu       *sync.Mutex
	putCount int // for testing
}

func NewLevelDBStore(dir string, maxFileHandles int) *LevelDBStore {
	d.Exp.NotEmpty(dir)
	d.Exp.NoError(os.MkdirAll(dir, 0700))
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: maxFileHandles,
		WriteBuffer:            1 << 24, // 16MiB
	})
	d.Chk.NoError(err)
	return &LevelDBStore{db, &sync.Mutex{}, 0}
}

func (l *LevelDBStore) Root() ref.Ref {
	val, err := l.db.Get([]byte(rootKey), nil)
	if err == errors.ErrNotFound {
		return ref.Ref{}
	}
	d.Chk.NoError(err)

	return ref.Parse(string(val))
}

func (l *LevelDBStore) UpdateRoot(current, last ref.Ref) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last != l.Root() {
		return false
	}

	// Sync: true write option should fsync memtable data to disk
	err := l.db.Put([]byte(rootKey), []byte(current.String()), &opt.WriteOptions{Sync: true})
	d.Chk.NoError(err)
	return true
}

func (l *LevelDBStore) Get(ref ref.Ref) io.ReadCloser {
	key := toChunkKey(ref)
	chunk, err := l.db.Get(key, nil)
	if err == errors.ErrNotFound {
		return nil
	}
	d.Chk.NoError(err)

	return ioutil.NopCloser(bytes.NewReader(chunk))
}

func (l *LevelDBStore) Has(ref ref.Ref) bool {
	key := toChunkKey(ref)
	exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	d.Chk.NoError(err)
	return exists
}

func (l *LevelDBStore) Put() ChunkWriter {
	return NewChunkWriter(l.write)
}

func (l *LevelDBStore) write(ref ref.Ref, data []byte) {
	if l.Has(ref) {
		return
	}

	key := toChunkKey(ref)
	err := l.db.Put(key, data, nil)
	d.Chk.NoError(err)
	l.putCount += 1
}

func (l *LevelDBStore) Close() error {
	l.db.Close()
	return nil
}

type ldbStoreFlags struct {
	dir            *string
	maxFileHandles *int
}

func levelDBFlags(prefix string) ldbStoreFlags {
	return ldbStoreFlags{
		flag.String(prefix+"ldb", "", "directory to use for a LevelDB-backed chunkstore"),
		flag.Int(prefix+"ldb-max-file-handles", 24, "max number of open file handles"),
	}
}

func (f ldbStoreFlags) createStore() ChunkStore {
	if *f.dir == "" {
		return nil
	} else {
		return NewLevelDBStore(*f.dir, *f.maxFileHandles)
	}
}
