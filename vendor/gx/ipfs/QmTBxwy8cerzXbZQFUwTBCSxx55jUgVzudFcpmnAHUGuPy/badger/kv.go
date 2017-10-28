/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"encoding/hex"
	"expvar"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"gx/ipfs/QmTEmsyNnckEq8rEfALfdhLHjrEHGoSGFDrAYReuetn7MC/go-net/trace"

	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/skl"
	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/table"
	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/y"
	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

var (
	badgerPrefix = []byte("!badger!")     // Prefix for internal keys used by badger.
	head         = []byte("!badger!head") // For storing value offset for replay.
)

type closers struct {
	updateSize *y.Closer
	compactors *y.Closer
	memtable   *y.Closer
	writes     *y.Closer
	valueGC    *y.Closer
}

// KV provides the various functions required to interact with Badger.
// KV is thread-safe.
type KV struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *DirectoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *DirectoryLockGuard

	closers   closers
	elog      trace.EventLog
	mt        *skl.Skiplist   // Our latest (actively written) in-memory table
	imm       []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	vptr      valuePointer // less than or equal to a pointer to the last vlog value put into mt
	writeCh   chan *request
	flushChan chan flushTask // For flushing memtables.

	// Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
	// we use an atomic op.
	lastUsedCasCounter uint64
}

// ErrInvalidDir is returned when Badger cannot find the directory
// from where it is supposed to load the key-value store.
var ErrInvalidDir = errors.New("Invalid Dir, directory does not exist")

// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
// range.
var ErrValueLogSize = errors.New("Invalid ValueLogFileSize, must be between 1MB and 2GB")

func exceedsMaxKeySizeError(key []byte) error {
	return errors.Errorf("Key with size %d exceeded %dMB limit. Key:\n%s",
		len(key), maxKeySize<<20, hex.Dump(key[:1<<10]))
}

func exceedsMaxValueSizeError(value []byte, maxValueSize int64) error {
	return errors.Errorf("Value with size %d exceeded ValueLogFileSize (%dMB). Key:\n%s",
		len(value), maxValueSize<<20, hex.Dump(value[:1<<10]))
}

const (
	kvWriteChCapacity = 1000
)

// NewKV returns a new KV object.
func NewKV(optParam *Options) (out *KV, err error) {
	// Make a copy early and fill in maxBatchSize
	opt := *optParam
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return nil, y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			return nil, ErrInvalidDir
		}
	}
	absDir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	absValueDir, err := filepath.Abs(opt.ValueDir)
	if err != nil {
		return nil, err
	}

	dirLockGuard, err := AcquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.Release()
		}
	}()
	var valueDirLockGuard *DirectoryLockGuard
	if absValueDir != absDir {
		valueDirLockGuard, err = AcquireDirectoryLock(opt.ValueDir, lockFile)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if valueDirLockGuard != nil {
			_ = valueDirLockGuard.Release()
		}
	}()
	if !(opt.ValueLogFileSize <= 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}
	manifestFile, manifest, err := openOrCreateManifestFile(opt.Dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	out = &KV{
		imm:           make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan:     make(chan flushTask, opt.NumMemtables),
		writeCh:       make(chan *request, kvWriteChCapacity),
		opt:           opt,
		manifest:      manifestFile,
		elog:          trace.NewEventLog("Badger", "KV"),
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
	}

	out.closers.updateSize = y.NewCloser(1)
	go out.updateSize(out.closers.updateSize)
	out.mt = skl.NewSkiplist(arenaSize(&opt))

	// newLevelsController potentially loads files in directory.
	if out.lc, err = newLevelsController(out, &manifest); err != nil {
		return nil, err
	}

	out.closers.compactors = y.NewCloser(1)
	out.lc.startCompact(out.closers.compactors)

	out.closers.memtable = y.NewCloser(1)
	go out.flushMemtable(out.closers.memtable) // Need levels controller to be up.

	if err = out.vlog.Open(out, &opt); err != nil {
		return nil, err
	}

	var item KVItem
	if err := out.Get(head, &item); err != nil {
		return nil, errors.Wrap(err, "Retrieving head")
	}

	var val []byte
	err = item.Value(func(v []byte) error {
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "Retrieving head value")
	}
	// lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
	// written value log entry that we replay.  (Subsequent value log entries might be _less_
	// than lastUsedCasCounter, if there was value log gc so we have to max() values while
	// replaying.)
	out.lastUsedCasCounter = item.casCounter

	var vptr valuePointer
	if len(val) > 0 {
		vptr.Decode(val)
	}

	replayCloser := y.NewCloser(1)
	go out.doWrites(replayCloser)

	first := true
	fn := func(e Entry, vp valuePointer) error { // Function for replaying.
		if first {
			out.elog.Printf("First key=%s\n", e.Key)
		}
		first = false
		if out.lastUsedCasCounter < e.casCounter {
			out.lastUsedCasCounter = e.casCounter
		}

		if e.CASCounterCheck != 0 {
			oldValue, err := out.get(e.Key)
			if err != nil {
				return err
			}
			if oldValue.CASCounter != e.CASCounterCheck {
				return nil
			}
		}
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if out.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = make([]byte, valuePointerEncodedSize)
			vp.Encode(nv)
			meta = meta | BitValuePointer
		}

		v := y.ValueStruct{
			Value:      nv,
			Meta:       meta,
			UserMeta:   e.UserMeta,
			CASCounter: e.casCounter,
		}
		for err := out.ensureRoomForWrite(); err != nil; err = out.ensureRoomForWrite() {
			out.elog.Printf("Replay: Making room for writes")
			time.Sleep(10 * time.Millisecond)
		}
		out.mt.Put(nk, v)
		return nil
	}
	if err = out.vlog.Replay(vptr, fn); err != nil {
		return out, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.

	// Mmap writable log
	lf := out.vlog.filesMap[out.vlog.maxFid]
	if err = lf.mmap(2 * out.vlog.opt.ValueLogFileSize); err != nil {
		return out, errors.Wrapf(err, "Unable to mmap RDWR log file")
	}

	out.writeCh = make(chan *request, kvWriteChCapacity)
	out.closers.writes = y.NewCloser(1)
	go out.doWrites(out.closers.writes)

	out.closers.valueGC = y.NewCloser(1)
	go out.vlog.waitOnGC(out.closers.valueGC)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return out, nil
}

// Close closes a KV. It's crucial to call it to ensure all the pending updates
// make their way to disk.
func (s *KV) Close() (err error) {
	s.elog.Printf("Closing database")
	// Stop value GC first.
	s.closers.valueGC.SignalAndWait()

	// Stop writes next.
	s.closers.writes.SignalAndWait()

	// Now close the value log.
	if vlogErr := s.vlog.Close(); err == nil {
		err = errors.Wrap(vlogErr, "KV.Close")
	}

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if !s.mt.Empty() {
		s.elog.Printf("Flushing memtable")
		for {
			pushedFlushTask := func() bool {
				s.Lock()
				defer s.Unlock()
				y.AssertTrue(s.mt != nil)
				select {
				case s.flushChan <- flushTask{s.mt, s.vptr}:
					s.imm = append(s.imm, s.mt) // Flusher will attempt to remove this from s.imm.
					s.mt = nil                  // Will segfault if we try writing!
					s.elog.Printf("pushed to flush chan\n")
					return true
				default:
					// If we fail to push, we need to unlock and wait for a short while.
					// The flushing operation needs to update s.imm. Otherwise, we have a deadlock.
					// TODO: Think about how to do this more cleanly, maybe without any locks.
				}
				return false
			}()
			if pushedFlushTask {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	s.flushChan <- flushTask{nil, valuePointer{}} // Tell flusher to quit.

	s.closers.memtable.Wait()
	s.elog.Printf("Memtable flushed")

	s.closers.compactors.SignalAndWait()
	s.elog.Printf("Compaction finished")

	if lcErr := s.lc.close(); err == nil {
		err = errors.Wrap(lcErr, "KV.Close")
	}
	s.elog.Printf("Waiting for closer")
	s.closers.updateSize.SignalAndWait()

	s.elog.Finish()

	if guardErr := s.dirLockGuard.Release(); err == nil {
		err = errors.Wrap(guardErr, "KV.Close")
	}
	if s.valueDirGuard != nil {
		if guardErr := s.valueDirGuard.Release(); err == nil {
			err = errors.Wrap(guardErr, "KV.Close")
		}
	}
	if manifestErr := s.manifest.close(); err == nil {
		err = errors.Wrap(manifestErr, "KV.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(s.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "KV.Close")
	}
	if syncErr := syncDir(s.opt.ValueDir); err == nil {
		err = errors.Wrap(syncErr, "KV.Close")
	}

	return err
}

const (
	lockFile = "LOCK"
)

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := OpenDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// getMemtables returns the current memtables and get references.
func (s *KV) getMemTables() ([]*skl.Skiplist, func()) {
	s.RLock()
	defer s.RUnlock()

	tables := make([]*skl.Skiplist, len(s.imm)+1)

	// Get mutable memtable.
	tables[0] = s.mt
	tables[0].IncrRef()

	// Get immutable memtables.
	last := len(s.imm) - 1
	for i := range s.imm {
		tables[i+1] = s.imm[last-i]
		tables[i+1].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (s *KV) yieldItemValue(item *KVItem, consumer func([]byte) error) error {
	if !item.hasValue() {
		return consumer(nil)
	}

	if item.slice == nil {
		item.slice = new(y.Slice)
	}

	if (item.meta & BitValuePointer) == 0 {
		val := item.slice.Resize(len(item.vptr))
		copy(val, item.vptr)
		return consumer(val)
	}

	var vp valuePointer
	vp.Decode(item.vptr)
	err := s.vlog.Read(vp, consumer)
	if err != nil {
		return err
	}
	return nil
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *KV) get(key []byte) (y.ValueStruct, error) {
	tables, decr := s.getMemTables() // Lock should be released.
	defer decr()

	y.NumGets.Add(1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGets.Add(1)
		if vs.Meta != 0 || vs.Value != nil {
			return vs, nil
		}
	}
	return s.lc.get(key)
}

// Get looks for key and returns a KVItem.
// If key is not found, item.Value() is nil.
func (s *KV) Get(key []byte, item *KVItem) error {
	vs, err := s.get(key)
	if err != nil {
		return errors.Wrapf(err, "KV::Get key: %q", key)
	}

	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.casCounter = vs.CASCounter
	item.key = key
	item.kv = s
	item.vptr = vs.Value

	return nil
}

// Exists looks if a key exists. Returns true if the
// key exists otherwises return false. if err is not nil an error occurs during
// the key lookup and the existence of the key is unknown
func (s *KV) Exists(key []byte) (bool, error) {
	vs, err := s.get(key)
	if err != nil {
		return false, err
	}

	if vs.Value == nil && vs.Meta == 0 {
		return false, nil
	}

	if (vs.Meta & BitDelete) != 0 {
		// Tombstone encountered.
		return false, nil
	}

	return true, nil
}

func (s *KV) updateOffset(ptrs []valuePointer) {
	var ptr valuePointer
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}

	s.Lock()
	defer s.Unlock()
	y.AssertTrue(!ptr.Less(s.vptr))
	s.vptr = ptr
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (s *KV) shouldWriteValueToLSM(e Entry) bool {
	return len(e.Value) < s.opt.ValueThreshold
}

func (s *KV) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		entry.Error = nil
		if entry.CASCounterCheck != 0 {
			oldValue, err := s.get(entry.Key)
			if err != nil {
				return errors.Wrap(err, "writeToLSM")
			}
			// No need to decode existing value. Just need old CAS counter.
			if oldValue.CASCounter != entry.CASCounterCheck {
				entry.Error = ErrCasMismatch
				continue
			}
		}

		if entry.Meta == BitSetIfAbsent {
			// Someone else might have written a value, so lets check again if key exists.
			exists, err := s.Exists(entry.Key)
			if err != nil {
				return err
			}
			// Value already exists, don't write.
			if exists {
				entry.Error = ErrKeyExists
				continue
			}
		}

		if s.shouldWriteValueToLSM(*entry) { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:      entry.Value,
					Meta:       entry.Meta,
					UserMeta:   entry.UserMeta,
					CASCounter: entry.casCounter})
		} else {
			var offsetBuf [valuePointerEncodedSize]byte
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:      b.Ptrs[i].Encode(offsetBuf[:]),
					Meta:       entry.Meta | BitValuePointer,
					UserMeta:   entry.UserMeta,
					CASCounter: entry.casCounter})
		}
	}
	return nil
}

// lastCASCounter returns the last-used cas counter.
func (s *KV) lastCASCounter() uint64 {
	return atomic.LoadUint64(&s.lastUsedCasCounter)
}

// newCASCounters generates a set of unique CAS counters -- the interval [x, x + howMany) where x
// is the return value.
func (s *KV) newCASCounters(howMany uint64) uint64 {
	last := atomic.AddUint64(&s.lastUsedCasCounter, howMany)
	return last - howMany + 1
}

// writeRequests is called serially by only one goroutine.
func (s *KV) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}

	s.elog.Printf("writeRequests called. Writing to value log")

	// CAS counter for all operations has to go onto value log. Otherwise, if it is just in
	// memtable for a long time, and following CAS operations use that as a check, when
	// replaying, we will think that these CAS operations should fail, when they are actually
	// valid.

	// There is code (in flushMemtable) whose correctness depends on us generating CAS Counter
	// values _before_ we modify s.vptr here.
	for _, req := range reqs {
		counterBase := s.newCASCounters(uint64(len(req.Entries)))
		for i, e := range req.Entries {
			e.casCounter = counterBase + uint64(i)
		}
	}
	err := s.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}

	s.elog.Printf("Writing to memtable")
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		for err := s.ensureRoomForWrite(); err != nil; err = s.ensureRoomForWrite() {
			s.elog.Printf("Making room for writes")
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := s.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		s.updateOffset(b.Ptrs)
	}
	done(nil)
	s.elog.Printf("%d entries written", count)
	return nil
}

func (s *KV) doWrites(lc *y.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := s.writeRequests(reqs); err != nil {
			log.Printf("ERROR in Badger::writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)
	y.PendingWrites.Set(s.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-s.writeCh:
		case <-lc.HasBeenClosed():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-s.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		close(s.writeCh)
		for r := range s.writeCh { // Flush the channel.
			reqs = append(reqs, r)
		}

		pendingCh <- struct{}{} // Push to pending before doing a write.
		writeRequests(reqs)
		return

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

func (s *KV) sendToWriteCh(entries []*Entry) []*request {
	var reqs []*request
	var count, size int64
	var b *request
	var bad []*Entry
	for _, entry := range entries {
		if len(entry.Key) > maxKeySize {
			entry.Error = exceedsMaxKeySizeError(entry.Key)
			bad = append(bad, entry)
			continue
		}
		if len(entry.Value) > int(s.opt.ValueLogFileSize) {
			entry.Error = exceedsMaxValueSizeError(entry.Value, s.opt.ValueLogFileSize)
			bad = append(bad, entry)
			continue
		}
		if b == nil {
			b = requestPool.Get().(*request)
			b.Entries = b.Entries[:0]
			b.Wg = sync.WaitGroup{}
			b.Wg.Add(1)
		}
		count++
		size += int64(s.opt.estimateSize(entry))
		b.Entries = append(b.Entries, entry)
		if count >= s.opt.maxBatchCount || size >= s.opt.maxBatchSize {
			s.writeCh <- b
			y.NumPuts.Add(int64(len(b.Entries)))
			reqs = append(reqs, b)
			count = 0
			size = 0
			b = nil
		}
	}

	if size > 0 {
		s.writeCh <- b
		y.NumPuts.Add(int64(len(b.Entries)))
		reqs = append(reqs, b)
	}

	if len(bad) > 0 {
		b := requestPool.Get().(*request)
		b.Entries = bad
		b.Wg = sync.WaitGroup{}
		b.Err = nil
		b.Ptrs = nil
		reqs = append(reqs, b)
		y.NumBlockedPuts.Add(int64(len(bad)))
	}

	return reqs
}

// BatchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned. Errors are also set on each Entry and must be checked
// individually.
//   Check(kv.BatchSet(entries))
//   for _, e := range entries {
//      Check(e.Error)
//   }
func (s *KV) BatchSet(entries []*Entry) error {
	reqs := s.sendToWriteCh(entries)

	var err error
	for _, req := range reqs {
		req.Wg.Wait()
		if req.Err != nil {
			err = req.Err
		}
		requestPool.Put(req)
	}
	return err
}

// BatchSetAsync is the asynchronous version of BatchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback. The caller should
// still check for errors set on each Entry individually.
//   kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//      for _, e := range entries {
//         Check(e.Error)
//      }
//   }
func (s *KV) BatchSetAsync(entries []*Entry, f func(error)) {
	reqs := s.sendToWriteCh(entries)

	go func() {
		var err error
		for _, req := range reqs {
			req.Wg.Wait()
			if req.Err != nil {
				err = req.Err
			}
			requestPool.Put(req)
		}
		// All writes complete, let's call the callback function now.
		f(err)
	}()
}

// Set sets the provided value for a given key. If key is not present, it is created.  If it is
// present, the existing value is overwritten with the one provided.
// Along with key and value, Set can also take an optional userMeta byte. This byte is stored
// alongside the key, and can be used as an aid to interpret the value or store other contextual
// bits corresponding to the key-value pair.
func (s *KV) Set(key, val []byte, userMeta byte) error {
	e := &Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	}
	if err := s.BatchSet([]*Entry{e}); err != nil {
		return err
	}
	return e.Error
}

// SetAsync is the asynchronous version of Set. It accepts a callback function which is called
// when the set is complete. Any error encountered during execution is passed as an argument
// to the callback function.
func (s *KV) SetAsync(key, val []byte, userMeta byte, f func(error)) {
	e := &Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	}
	s.BatchSetAsync([]*Entry{e}, func(err error) {
		if err != nil {
			f(err)
			return
		}
		if e.Error != nil {
			f(e.Error)
			return
		}
		f(nil)
	})
}

func (s *KV) setIfAbsent(key, val []byte, userMeta byte) (*Entry, error) {
	exists, err := s.Exists(key)
	if err != nil {
		return nil, err
	}
	// Found the key, return KeyExists
	if exists {
		return nil, ErrKeyExists
	}

	e := &Entry{
		Key:      key,
		Meta:     BitSetIfAbsent,
		Value:    val,
		UserMeta: userMeta,
	}
	return e, nil
}

// SetIfAbsent sets value of key if key is not present.
// If it is present, it returns the KeyExists error.
func (s *KV) SetIfAbsent(key, val []byte, userMeta byte) error {
	e, err := s.setIfAbsent(key, val, userMeta)
	if err != nil {
		return err
	}

	if err := s.BatchSet([]*Entry{e}); err != nil {
		return err
	}
	return e.Error
}

// SetIfAbsentAsync is the asynchronous version of SetIfAbsent. It accepts a callback function which
// is called when the operation is complete. Any error encountered during execution is passed as an
// argument to the callback function.
func (s *KV) SetIfAbsentAsync(key, val []byte, userMeta byte, f func(error)) error {
	e, err := s.setIfAbsent(key, val, userMeta)
	if err != nil {
		return err
	}

	s.BatchSetAsync([]*Entry{e}, func(err error) {
		if err != nil {
			f(err)
			return
		}
		if e.Error != nil {
			f(e.Error)
			return
		}
		f(nil)
	})
	return nil
}

// EntriesSet adds a Set to the list of entries.
// Exposing this so that user does not have to specify the Entry directly.
func EntriesSet(s []*Entry, key, val []byte) []*Entry {
	return append(s, &Entry{
		Key:   key,
		Value: val,
	})
}

// CompareAndSet sets the given value, ensuring that the no other Set operation has happened,
// since last read. If the key has a different casCounter, this would not update the key
// and return an error.
func (s *KV) CompareAndSet(key []byte, val []byte, casCounter uint64) error {
	e := &Entry{
		Key:             key,
		Value:           val,
		CASCounterCheck: casCounter,
	}
	if err := s.BatchSet([]*Entry{e}); err != nil {
		return err
	}
	return e.Error
}

func (s *KV) compareAsync(e *Entry, f func(error)) {
	b := requestPool.Get().(*request)
	b.Wg = sync.WaitGroup{}
	b.Wg.Add(1)
	s.writeCh <- b

	go func() {
		b.Wg.Wait()
		if b.Err != nil {
			f(b.Err)
			return
		}
		f(e.Error)
	}()
}

// CompareAndSetAsync is the asynchronous version of CompareAndSet. It accepts a callback function
// which is called when the CompareAndSet completes. Any error encountered during execution is
// passed as an argument to the callback function.
func (s *KV) CompareAndSetAsync(key []byte, val []byte, casCounter uint64, f func(error)) {
	e := &Entry{
		Key:             key,
		Value:           val,
		CASCounterCheck: casCounter,
	}
	s.compareAsync(e, f)
}

// Delete deletes a key.
// Exposing this so that user does not have to specify the Entry directly.
// For example, BitDelete seems internal to badger.
func (s *KV) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}

	return s.BatchSet([]*Entry{e})
}

// DeleteAsync is the asynchronous version of Delete. It calls the callback function after deletion
// is complete. Any error encountered during the execution is passed as an argument to the
// callback function.
func (s *KV) DeleteAsync(key []byte, f func(error)) {
	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}
	s.BatchSetAsync([]*Entry{e}, f)
}

// EntriesDelete adds a Del to the list of entries.
func EntriesDelete(s []*Entry, key []byte) []*Entry {
	return append(s, &Entry{
		Key:  key,
		Meta: BitDelete,
	})
}

// CompareAndDelete deletes a key ensuring that it has not been changed since last read.
// If existing key has different casCounter, this would not delete the key and return an error.
func (s *KV) CompareAndDelete(key []byte, casCounter uint64) error {
	e := &Entry{
		Key:             key,
		Meta:            BitDelete,
		CASCounterCheck: casCounter,
	}
	if err := s.BatchSet([]*Entry{e}); err != nil {
		return err
	}
	return e.Error
}

// CompareAndDeleteAsync is the asynchronous version of CompareAndDelete. It accepts a callback
// function which is called when the CompareAndDelete completes. Any error encountered during
// execution is passed as an argument to the callback function.
func (s *KV) CompareAndDeleteAsync(key []byte, casCounter uint64, f func(error)) {
	e := &Entry{
		Key:             key,
		Meta:            BitDelete,
		CASCounterCheck: casCounter,
	}
	s.compareAsync(e, f)
}

var errNoRoom = errors.New("No room for write")

// ensureRoomForWrite is always called serially.
func (s *KV) ensureRoomForWrite() error {
	var err error
	s.Lock()
	defer s.Unlock()
	if s.mt.MemSize() < s.opt.MaxTableSize {
		return nil
	}

	y.AssertTrue(s.mt != nil) // A nil mt indicates that KV is being closed.
	select {
	case s.flushChan <- flushTask{s.mt, s.vptr}:
		s.elog.Printf("Flushing value log to disk if async mode.")
		// Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
		err = s.vlog.sync()
		if err != nil {
			return err
		}

		s.elog.Printf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
			s.mt.MemSize(), len(s.flushChan))
		// We manage to push this task. Let's modify imm.
		s.imm = append(s.imm, s.mt)
		s.mt = skl.NewSkiplist(arenaSize(&s.opt))
		// New memtable is empty. We certainly have room.
		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return errNoRoom
	}
}

func arenaSize(opt *Options) int64 {
	return opt.MaxTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func writeLevel0Table(s *skl.Skiplist, f *os.File) error {
	iter := s.NewIterator()
	defer iter.Close()
	b := table.NewTableBuilder()
	defer b.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	_, err := f.Write(b.Finish())
	return err
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

func (s *KV) flushMemtable(lc *y.Closer) error {
	defer lc.Done()

	for ft := range s.flushChan {
		if ft.mt == nil {
			return nil
		}

		if !ft.vptr.IsZero() {
			s.elog.Printf("Storing offset: %+v\n", ft.vptr)
			offset := make([]byte, valuePointerEncodedSize)
			ft.vptr.Encode(offset)
			// CAS counter is needed and is desirable -- it's the first value log entry
			// we replay, so to speak, perhaps the only, and we use it to re-initialize
			// the CAS counter.
			//
			// The write loop generates CAS counter values _before_ it sets vptr.  It
			// is crucial that we read the cas counter here _after_ reading vptr.  That
			// way, our value here is guaranteed to be >= the CASCounter values written
			// before vptr (because they don't get replayed).
			ft.mt.Put(head, y.ValueStruct{Value: offset, CASCounter: s.lastCASCounter()})
		}
		fileID := s.lc.reserveFileID()
		fd, err := y.CreateSyncedFile(table.NewFilename(fileID, s.opt.Dir), true)
		if err != nil {
			return y.Wrap(err)
		}

		// Don't block just to sync the directory entry.
		dirSyncCh := make(chan error)
		go func() { dirSyncCh <- syncDir(s.opt.Dir) }()

		err = writeLevel0Table(ft.mt, fd)
		dirSyncErr := <-dirSyncCh

		if err != nil {
			s.elog.Errorf("ERROR while writing to level 0: %v", err)
			return err
		}
		if dirSyncErr != nil {
			s.elog.Errorf("ERROR while syncing level directory: %v", dirSyncErr)
			return err
		}

		tbl, err := table.OpenTable(fd, s.opt.TableLoadingMode)
		if err != nil {
			s.elog.Printf("ERROR while opening table: %v", err)
			return err
		}
		// We own a ref on tbl.
		err = s.lc.addLevel0Table(tbl) // This will incrRef (if we don't error, sure)
		tbl.DecrRef()                  // Releases our ref.
		if err != nil {
			return err
		}

		// Update s.imm. Need a lock.
		s.Lock()
		y.AssertTrue(ft.mt == s.imm[0]) //For now, single threaded.
		s.imm = s.imm[1:]
		ft.mt.DecrRef() // Return memory.
		s.Unlock()
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (s *KV) updateSize(lc *y.Closer) {
	defer lc.Done()

	metricsTicker := time.NewTicker(5 * time.Minute)
	defer metricsTicker.Stop()

	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if ext == ".sst" {
				lsmSize += info.Size()
			} else if ext == ".vlog" {
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			s.elog.Printf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	for {
		select {
		case <-metricsTicker.C:
			lsmSize, vlogSize := totalSize(s.opt.Dir)
			y.LSMSize.Set(s.opt.Dir, newInt(lsmSize))
			// If valueDir is different from dir, we'd have to do another walk.
			if s.opt.ValueDir != s.opt.Dir {
				_, vlogSize = totalSize(s.opt.ValueDir)
			}
			y.VlogSize.Set(s.opt.Dir, newInt(vlogSize))
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// RunValueLogGC would trigger a value log garbage collection with no guarantees that a call would
// result in a space reclaim. Every run would in the best case rewrite only one log file. So,
// repeated calls may be necessary.
//
// The way it currently works is that it would randomly pick up a value log file, and sample it. If
// the sample shows that we can discard at least discardRatio space of that file, it would be
// rewritten. Else, an ErrNoRewrite error would be returned indicating that the GC didn't result in
// any file rewrite.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the
// space can be discarded.  This results in a lifetime value log write amplification of 2 (1 from
// original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result
// in fewer space reclaims, while setting it to a lower value would result in more space reclaims at
// the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0),
// both endpoints excluded, otherwise an ErrInvalidRequest is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or KV has been closed, this
// would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
func (s *KV) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return ErrInvalidRequest
	}
	return s.vlog.runGC(discardRatio)
}
