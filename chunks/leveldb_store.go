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

var (
	rootKey     = []byte("/root")
	chunkPrefix = []byte("/chunk/")
)

type LevelDBStore struct {
	db                                     *leveldb.DB
	mu                                     *sync.Mutex
	concurrentWriteLimit                   chan struct{}
	getCount, hasCount, putCount, putBytes int64
	dumpStats                              bool
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
	d.Chk.NoError(err, "opening LevelDBStore in %s", dir)
	return &LevelDBStore{
		db:                   db,
		mu:                   &sync.Mutex{},
		concurrentWriteLimit: make(chan struct{}, maxFileHandles),
		dumpStats:            dumpStats,
	}
}

func (l *LevelDBStore) Root() ref.Ref {
	return l.rootByKey(rootKey)
}

func (l *LevelDBStore) rootByKey(key []byte) ref.Ref {
	val, err := l.db.Get(key, nil)
	if err == errors.ErrNotFound {
		return ref.Ref{}
	}
	d.Chk.NoError(err)

	return ref.Parse(string(val))
}

func (l *LevelDBStore) UpdateRoot(current, last ref.Ref) bool {
	return l.updateRootByKey(rootKey, current, last)
}

func (l *LevelDBStore) updateRootByKey(key []byte, current, last ref.Ref) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last != l.rootByKey(key) {
		return false
	}

	// Sync: true write option should fsync memtable data to disk
	err := l.db.Put(key, []byte(current.String()), &opt.WriteOptions{Sync: true})
	d.Chk.NoError(err)
	return true
}

func (l *LevelDBStore) Get(ref ref.Ref) Chunk {
	return l.getByKey(toChunkKey(ref), ref)
}

func (l *LevelDBStore) getByKey(key []byte, ref ref.Ref) Chunk {
	data, err := l.db.Get(key, nil)
	l.getCount++
	if err == errors.ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)

	return NewChunkWithRef(ref, data)
}

func (l *LevelDBStore) Has(ref ref.Ref) bool {
	return l.hasByKey(toChunkKey(ref))
}

func (l *LevelDBStore) hasByKey(key []byte) bool {
	exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *LevelDBStore) Put(c Chunk) {
	l.putByKey(toChunkKey(c.Ref()), c)
}

func (l *LevelDBStore) putByKey(key []byte, c Chunk) {
	l.concurrentWriteLimit <- struct{}{}
	err := l.db.Put(key, c.Data(), nil)
	d.Chk.NoError(err)
	l.putCount++
	l.putBytes += int64(len(c.Data()))
	<-l.concurrentWriteLimit
}

func (l *LevelDBStore) Close() error {
	l.db.Close()
	if l.dumpStats {
		fmt.Println("--LevelDB Stats--")
		fmt.Println("GetCount: ", l.getCount)
		fmt.Println("HasCount: ", l.hasCount)
		fmt.Println("PutCount: ", l.putCount)
		fmt.Println("Average PutSize: ", l.putBytes/l.putCount)
	}
	return nil
}

func toChunkKey(r ref.Ref) []byte {
	digest := r.Digest()
	return append(chunkPrefix, digest[:]...)
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
	if f.check() {
		return NewLevelDBStore(*f.dir, *f.maxFileHandles, *f.dumpStats)
	}
	return nil
}

func (f LevelDBStoreFlags) CreateFactory() Factory {
	if f.check() {
		return &LevelDBStoreFactory{f, f.CreateStore().(*LevelDBStore)}
	}
	return nil
}

func (f LevelDBStoreFlags) check() bool {
	return *f.dir != ""
}

type LevelDBStoreFactory struct {
	flags LevelDBStoreFlags
	store *LevelDBStore
}

func (f *LevelDBStoreFactory) CreateNamespacedStore(ns string) ChunkStore {
	d.Chk.NotNil(f.store, "Cannot use LevelDBStoreFactory after Shutter().")
	if f.flags.check() {
		return &NamedLevelDBStore{f.store, []byte(ns)}
	}
	return nil
}

func (f *LevelDBStoreFactory) Shutter() {
	f.store.Close()
	f.store = nil
}

type NamedLevelDBStore struct {
	*LevelDBStore
	namespace []byte
}

func (l *NamedLevelDBStore) Root() ref.Ref {
	d.Chk.NotNil(l.LevelDBStore, "Cannot use NamedLevelDBStore after Close().")
	return l.rootByKey(append(l.namespace, rootKey...))
}

func (l *NamedLevelDBStore) UpdateRoot(current, last ref.Ref) bool {
	d.Chk.NotNil(l.LevelDBStore, "Cannot use NamedLevelDBStore after Close().")
	return l.updateRootByKey(append(l.namespace, rootKey...), current, last)
}

func (l *NamedLevelDBStore) Get(ref ref.Ref) Chunk {
	d.Chk.NotNil(l.LevelDBStore, "Cannot use NamedLevelDBStore after Close().")
	return l.getByKey(l.toChunkKey(ref), ref)
}

func (l *NamedLevelDBStore) Has(ref ref.Ref) bool {
	d.Chk.NotNil(l.LevelDBStore, "Cannot use NamedLevelDBStore after Close().")
	return l.hasByKey(l.toChunkKey(ref))
}

func (l *NamedLevelDBStore) Put(c Chunk) {
	d.Chk.NotNil(l.LevelDBStore, "Cannot use NamedLevelDBStore after Close().")
	l.putByKey(l.toChunkKey(c.Ref()), c)
}

func (l *NamedLevelDBStore) Close() error {
	l.LevelDBStore = nil
	return nil
}

func (l *NamedLevelDBStore) toChunkKey(r ref.Ref) []byte {
	return append(l.namespace, toChunkKey(r)...)
}
