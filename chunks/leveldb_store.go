package chunks

import (
	"flag"
	"fmt"
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
	db                           *leveldb.DB
	mu                           *sync.Mutex
	concurrentWriteLimit         chan struct{}
	getCount, hasCount, putCount int
	dumpStats                    bool
}

func NewLevelDBStore(dir string, maxFileHandles int, dumpStats bool) *LevelDBStore {
	d.Exp.NotEmpty(dir)
	d.Exp.NoError(os.MkdirAll(dir, 0700))
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: maxFileHandles,
		WriteBuffer:            1 << 24, // 16MiB,
	})
	d.Chk.NoError(err)
	return &LevelDBStore{
		db:                   db,
		mu:                   &sync.Mutex{},
		concurrentWriteLimit: make(chan struct{}, maxFileHandles),
		dumpStats:            dumpStats,
	}
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

func (l *LevelDBStore) Get(ref ref.Ref) Chunk {
	key := toChunkKey(ref)
	data, err := l.db.Get(key, nil)
	l.getCount++
	if err == errors.ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)

	return NewChunkWithRef(ref, data)
}

func (l *LevelDBStore) Has(ref ref.Ref) bool {
	key := toChunkKey(ref)
	exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *LevelDBStore) Put(c Chunk) {
	if l.Has(c.Ref()) {
		return
	}

	l.concurrentWriteLimit <- struct{}{}
	err := l.db.Put(toChunkKey(c.Ref()), c.Data(), nil)
	d.Chk.NoError(err)
	l.putCount++
	<-l.concurrentWriteLimit
}

func (l *LevelDBStore) Close() error {
	l.db.Close()
	if l.dumpStats {
		fmt.Println("--LevelDB Stats--")
		fmt.Println("GetCount: ", l.getCount)
		fmt.Println("HasCount: ", l.hasCount)
		fmt.Println("PutCount: ", l.putCount)
	}
	return nil
}

type LevelDBStoreFlags struct {
	dir            *string
	maxFileHandles *int
	dumpStats      *bool
}

func LevelDBFlags(prefix string) LevelDBStoreFlags {
	return LevelDBStoreFlags{
		flag.String(prefix+"ldb", "", "directory to use for a LevelDB-backed chunkstore"),
		flag.Int(prefix+"ldb-max-file-handles", 24, "max number of open file handles"),
		flag.Bool(prefix+"ldb-dump-stats", false, "print get/has/put counts on close"),
	}
}

func (f LevelDBStoreFlags) CreateStore() ChunkStore {
	if *f.dir == "" {
		return nil
	} else {
		return NewLevelDBStore(*f.dir, *f.maxFileHandles, *f.dumpStats)
	}
}
