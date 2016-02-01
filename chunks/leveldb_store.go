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

const (
	rootKeyConst     = "/root"
	chunkPrefixConst = "/chunk/"
)

func NewLevelDBStore(dir, name string, maxFileHandles int, dumpStats bool) *LevelDBStore {
	return newLevelDBStore(newBackingStore(dir, maxFileHandles, dumpStats), []byte(name), true)
}

func newLevelDBStore(store *internalLevelDBStore, name []byte, closeBackingStore bool) *LevelDBStore {
	return &LevelDBStore{
		internalLevelDBStore: store,
		rootKey:              append(name, []byte(rootKeyConst)...),
		chunkPrefix:          append(name, []byte(chunkPrefixConst)...),
		closeBackingStore:    closeBackingStore,
	}
}

type LevelDBStore struct {
	*internalLevelDBStore
	rootKey           []byte
	chunkPrefix       []byte
	closeBackingStore bool
}

func (l *LevelDBStore) Root() ref.Ref {
	d.Chk.NotNil(l.internalLevelDBStore, "Cannot use LevelDBStore after Close().")
	return l.rootByKey(l.rootKey)
}

func (l *LevelDBStore) UpdateRoot(current, last ref.Ref) bool {
	d.Chk.NotNil(l.internalLevelDBStore, "Cannot use LevelDBStore after Close().")
	return l.updateRootByKey(l.rootKey, current, last)
}

func (l *LevelDBStore) Get(ref ref.Ref) Chunk {
	d.Chk.NotNil(l.internalLevelDBStore, "Cannot use LevelDBStore after Close().")
	return l.getByKey(l.toChunkKey(ref), ref)
}

func (l *LevelDBStore) Has(ref ref.Ref) bool {
	d.Chk.NotNil(l.internalLevelDBStore, "Cannot use LevelDBStore after Close().")
	return l.hasByKey(l.toChunkKey(ref))
}

func (l *LevelDBStore) Put(c Chunk) {
	d.Chk.NotNil(l.internalLevelDBStore, "Cannot use LevelDBStore after Close().")
	l.putByKey(l.toChunkKey(c.Ref()), c)
}

func (l *LevelDBStore) Close() error {
	if l.closeBackingStore {
		l.internalLevelDBStore.Close()
	}
	l.internalLevelDBStore = nil
	return nil
}

func (l *LevelDBStore) toChunkKey(r ref.Ref) []byte {
	digest := r.DigestSlice()
	out := make([]byte, len(l.chunkPrefix), len(l.chunkPrefix)+len(digest))
	copy(out, l.chunkPrefix)
	return append(out, digest...)
}

type internalLevelDBStore struct {
	db                                     *leveldb.DB
	mu                                     *sync.Mutex
	concurrentWriteLimit                   chan struct{}
	getCount, hasCount, putCount, putBytes int64
	dumpStats                              bool
}

func newBackingStore(dir string, maxFileHandles int, dumpStats bool) *internalLevelDBStore {
	d.Exp.NotEmpty(dir)
	d.Exp.NoError(os.MkdirAll(dir, 0700))
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10), // 10 bits/key
		OpenFilesCacheCapacity: maxFileHandles,
		WriteBuffer:            1 << 24, // 16MiB,
	})
	d.Chk.NoError(err, "opening internalLevelDBStore in %s", dir)
	return &internalLevelDBStore{
		db:                   db,
		mu:                   &sync.Mutex{},
		concurrentWriteLimit: make(chan struct{}, maxFileHandles),
		dumpStats:            dumpStats,
	}
}

func (l *internalLevelDBStore) rootByKey(key []byte) ref.Ref {
	val, err := l.db.Get(key, nil)
	if err == errors.ErrNotFound {
		return ref.Ref{}
	}
	d.Chk.NoError(err)

	return ref.Parse(string(val))
}

func (l *internalLevelDBStore) updateRootByKey(key []byte, current, last ref.Ref) bool {
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

func (l *internalLevelDBStore) getByKey(key []byte, ref ref.Ref) Chunk {
	data, err := l.db.Get(key, nil)
	l.getCount++
	if err == errors.ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)

	return NewChunkWithRef(ref, data)
}

func (l *internalLevelDBStore) hasByKey(key []byte) bool {
	exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *internalLevelDBStore) putByKey(key []byte, c Chunk) {
	l.concurrentWriteLimit <- struct{}{}
	err := l.db.Put(key, c.Data(), nil)
	d.Chk.NoError(err)
	l.putCount++
	l.putBytes += int64(len(c.Data()))
	<-l.concurrentWriteLimit
}

func (l *internalLevelDBStore) Close() error {
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

func (f LevelDBStoreFlags) CreateStore(ns string) ChunkStore {
	if f.check() {
		return NewLevelDBStore(*f.dir, ns, *f.maxFileHandles, *f.dumpStats)
	}
	return nil
}

func (f LevelDBStoreFlags) CreateFactory() Factory {
	if f.check() {
		return &LevelDBStoreFactory{f, newBackingStore(*f.dir, *f.maxFileHandles, *f.dumpStats)}
	}
	return nil
}

func (f LevelDBStoreFlags) check() bool {
	return *f.dir != ""
}

type LevelDBStoreFactory struct {
	flags LevelDBStoreFlags
	store *internalLevelDBStore
}

func (f *LevelDBStoreFactory) CreateStore(ns string) ChunkStore {
	d.Chk.NotNil(f.store, "Cannot use LevelDBStoreFactory after Shutter().")
	if f.flags.check() {
		return newLevelDBStore(f.store, []byte(ns), false)
	}
	return nil
}

func (f *LevelDBStoreFactory) Shutter() {
	f.store.Close()
	f.store = nil
}
