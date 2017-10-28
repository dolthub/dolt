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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/y"
	"gx/ipfs/QmTEmsyNnckEq8rEfALfdhLHjrEHGoSGFDrAYReuetn7MC/go-net/trace"
	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete       byte  = 1 // Set if the key has been deleted.
	BitValuePointer byte  = 2 // Set if the value is NOT stored directly next to key.
	BitUnused       byte  = 4
	BitSetIfAbsent  byte  = 8 // Set if the key is set using SetIfAbsent.
	M               int64 = 1 << 20
)

var (
	// ErrRetry is returned when a log file containing the value is not found.
	// This usually indicates that it may have been garbage collected, and the
	// operation needs to be retried.
	ErrRetry = errors.New("Unable to find log file. Please retry")

	// ErrCasMismatch is returned when a CompareAndSet operation has failed due
	// to a counter mismatch.
	ErrCasMismatch = errors.New("CompareAndSet failed due to counter mismatch")

	// ErrKeyExists is returned by SetIfAbsent metadata bit is set, but the
	// key already exists in the store.
	ErrKeyExists = errors.New("SetIfAbsent failed since key already exists")

	// ErrThresholdZero is returned if threshold is set to zero, and value log GC is called.
	// In such a case, GC can't be run.
	ErrThresholdZero = errors.New(
		"Value log GC can't run because threshold is set to zero")

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New(
		"Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after KV::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
)

const maxKeySize = 1 << 20

type logFile struct {
	path string
	// This is a lock on the log file. It guards the fd’s value, the file’s
	// existence and the file’s memory map.
	//
	// Use shared ownership when reading/writing the file or memory map, use
	// exclusive ownership to open/close the descriptor, unmap or remove the file.
	lock sync.RWMutex
	fd   *os.File
	fid  uint32
	fmap []byte
	size uint32
}

// openReadOnly assumes that we have a write lock on logFile.
func (lf *logFile) openReadOnly() error {
	var err error
	lf.fd, err = os.OpenFile(lf.path, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open %q as RDONLY.", lf.path)
	}

	fi, err := lf.fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.path)
	}
	lf.size = uint32(fi.Size())

	if err = lf.mmap(fi.Size()); err != nil {
		_ = lf.fd.Close()
		return y.Wrapf(err, "Unable to map file")
	}

	return nil
}

func (lf *logFile) mmap(size int64) (err error) {
	lf.fmap, err = y.Mmap(lf.fd, false, size)
	if err == nil {
		err = y.Madvise(lf.fmap, false) // Disable readahead
	}
	return err
}

var errTooFewBytes = errors.New("Too few bytes read")

// Acquire lock on mmap if you are calling this
func (lf *logFile) read(p valuePointer) (buf []byte, err error) {
	var nbr int64
	offset := p.Offset
	size := uint32(len(lf.fmap))
	valsz := p.Len
	if offset >= size || offset+valsz > size {
		err = y.ErrEOF
	} else {
		buf = lf.fmap[offset : offset+valsz]
		nbr = int64(valsz)
	}
	y.NumReads.Add(1)
	y.NumBytesRead.Add(nbr)
	return buf, err
}

func (lf *logFile) doneWriting(offset uint32) error {
	// Sync before acquiring lock.  (We call this from write() and thus know we have shared access
	// to the fd.)
	if err := lf.fd.Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.path)
	}
	// Close and reopen the file read-only.  Acquire lock because fd will become invalid for a bit.
	// Acquiring the lock is bad because, while we don't hold the lock for a long time, it forces
	// one batch of readers wait for the preceding batch of readers to finish.
	//
	// If there's a benefit to reopening the file read-only, it might be on Windows.  I don't know
	// what the benefit is.  Consider keeping the file read-write, or use fcntl to change
	// permissions.
	lf.lock.Lock()
	defer lf.lock.Unlock()
	if err := y.Munmap(lf.fmap); err != nil {
		return errors.Wrapf(err, "Unable to munmap value log: %q", lf.path)
	}
	// TODO: Confirm if we need to run a file sync after truncation.
	// Truncation must run after unmapping, otherwise Windows would crap itself.
	if err := lf.fd.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.path)
	}
	if err := lf.fd.Close(); err != nil {
		return errors.Wrapf(err, "Unable to close value log: %q", lf.path)
	}

	return lf.openReadOnly()
}

// You must hold lf.lock to sync()
func (lf *logFile) sync() error {
	return lf.fd.Sync()
}

var errStop = errors.New("Stop iteration")

type logEntry func(e Entry, vp valuePointer) error

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (vlog *valueLog) iterate(lf *logFile, offset uint32, fn logEntry) error {
	_, err := lf.fd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return y.Wrap(err)
	}

	reader := bufio.NewReader(lf.fd)
	var hbuf [headerBufSize]byte
	var h header
	k := make([]byte, 1<<10)
	v := make([]byte, 1<<20)

	truncate := false
	recordOffset := offset
	for {
		hash := crc32.New(y.CastagnoliCrcTable)
		tee := io.TeeReader(reader, hash)

		if _, err = io.ReadFull(tee, hbuf[:]); err != nil {
			if err == io.EOF {
				break
			} else if err == io.ErrUnexpectedEOF {
				truncate = true
				break
			}
			return err
		}

		var e Entry
		e.offset = recordOffset
		h.Decode(hbuf[:])
		if h.klen > maxKeySize {
			truncate = true
			break
		}
		vl := int(h.vlen)
		if cap(v) < vl {
			v = make([]byte, 2*vl)
		}

		kl := int(h.klen)
		if cap(k) < kl {
			k = make([]byte, 2*kl)
		}
		e.Key = k[:kl]
		e.Value = v[:vl]

		if _, err = io.ReadFull(tee, e.Key); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncate = true
				break
			}
			return err
		}
		e.Meta = h.meta
		e.UserMeta = h.userMeta
		e.casCounter = h.casCounter
		e.CASCounterCheck = h.casCounterCheck
		if _, err = io.ReadFull(tee, e.Value); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncate = true
				break
			}
			return err
		}

		var crcBuf [4]byte
		if _, err = io.ReadFull(reader, crcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncate = true
				break
			}
			return err
		}
		crc := binary.BigEndian.Uint32(crcBuf[:])
		if crc != hash.Sum32() {
			truncate = true
			break
		}

		var vp valuePointer

		vp.Len = headerBufSize + h.klen + h.vlen + uint32(len(crcBuf))
		recordOffset += vp.Len

		vp.Offset = e.offset
		vp.Fid = lf.fid

		if err := fn(e, vp); err != nil {
			if err == errStop {
				break
			}
			return y.Wrap(err)
		}
	}

	if truncate && len(lf.fmap) == 0 {
		// Only truncate if the file isn't mmaped. Otherwise, Windows would puke.
		if err := lf.fd.Truncate(int64(recordOffset)); err != nil {
			return err
		}
	}

	return nil
}

func (vlog *valueLog) rewrite(f *logFile) error {
	maxFid := atomic.LoadUint32(&vlog.maxFid)
	y.AssertTruef(uint32(f.fid) < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)

	elog := trace.NewEventLog("badger", "vlog-rewrite")
	defer elog.Finish()
	elog.Printf("Rewriting fid: %d", f.fid)

	wb := make([]*Entry, 0, 1000)
	var size int64

	y.AssertTrue(vlog.kv != nil)
	var count int
	fe := func(e Entry) error {
		count++
		if count%10000 == 0 {
			elog.Printf("Processing entry %d", count)
		}

		vs, err := vlog.kv.get(e.Key)
		if err != nil {
			return err
		}

		if (vs.Meta & BitDelete) > 0 {
			return nil
		}
		if (vs.Meta & BitValuePointer) == 0 {
			return nil
		}

		// Value is still present in value log.
		if len(vs.Value) == 0 {
			return errors.Errorf("Empty value: %+v", vs)
		}
		var vp valuePointer
		vp.Decode(vs.Value)

		if vp.Fid > f.fid {
			return nil
		}
		if vp.Offset > e.offset {
			return nil
		}
		if vp.Fid == f.fid && vp.Offset == e.offset {
			// This new entry only contains the key, and a pointer to the value.
			ne := new(Entry)
			if e.Meta == BitSetIfAbsent {
				// If we rewrite this entry without removing BitSetIfAbsent, lsm would see that
				// the key is already present, which would be this same entry and won't update
				// the vptr to point to the new file.
				e.Meta = 0
			}
			y.AssertTruef(e.Meta == 0, "Got meta: 0")
			ne.Meta = e.Meta
			ne.UserMeta = e.UserMeta
			ne.Key = make([]byte, len(e.Key))
			copy(ne.Key, e.Key)
			ne.Value = make([]byte, len(e.Value))
			copy(ne.Value, e.Value)
			// CAS counter check. Do not rewrite if key has a newer value.
			ne.CASCounterCheck = vs.CASCounter
			wb = append(wb, ne)
			size += int64(vlog.opt.estimateSize(ne))
			if size >= 64*M {
				elog.Printf("request has %d entries, size %d", len(wb), size)
				if err := vlog.kv.BatchSet(wb); err != nil {
					return err
				}
				size = 0
				wb = wb[:0]
			}
		} else {
			// This can now happen because we can move some entries forward, but then not write
			// them to LSM tree due to CAS check failure.
		}
		return nil
	}

	err := vlog.iterate(f, 0, func(e Entry, vp valuePointer) error {
		return fe(e)
	})
	if err != nil {
		return err
	}

	if len(wb) > 0 {
		elog.Printf("request has %d entries, size %d", len(wb), size)
		if err := vlog.kv.BatchSet(wb); err != nil {
			return err
		}
	}
	elog.Printf("Processed %d entries in total", count)

	elog.Printf("Removing fid: %d", f.fid)
	var deleteFileNow bool
	// Entries written to LSM. Remove the older file now.
	{
		vlog.filesLock.Lock()
		// Just a sanity-check.
		if _, ok := vlog.filesMap[f.fid]; !ok {
			vlog.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", f.fid)
		}
		if vlog.numActiveIterators == 0 {
			delete(vlog.filesMap, f.fid)
			deleteFileNow = true
		} else {
			vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, f.fid)
		}
		vlog.filesLock.Unlock()
	}

	if deleteFileNow {
		vlog.deleteLogFile(f)
	}

	return nil
}

func (vlog *valueLog) incrIteratorCount() {
	vlog.filesLock.Lock()
	vlog.numActiveIterators++
	vlog.filesLock.Unlock()
}

func (vlog *valueLog) decrIteratorCount() error {
	vlog.filesLock.Lock()

	vlog.numActiveIterators--
	if vlog.numActiveIterators != 0 {
		vlog.filesLock.Unlock()
		return nil
	}
	lfs := make([]*logFile, 0, len(vlog.filesToBeDeleted))
	for _, id := range vlog.filesToBeDeleted {
		lfs = append(lfs, vlog.filesMap[id])
		delete(vlog.filesMap, id)
	}
	vlog.filesToBeDeleted = nil
	vlog.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vlog.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) deleteLogFile(lf *logFile) error {
	path := vlog.fpath(lf.fid)
	if err := y.Munmap(lf.fmap); err != nil {
		_ = lf.fd.Close()
		return err
	}
	if err := lf.fd.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}

// Entry provides Key, Value and if required, CASCounterCheck to kv.BatchSet() API.
// If CASCounterCheck is provided, it would be compared against the current casCounter
// assigned to this key-value. Set be done on this key only if the counters match.
type Entry struct {
	Key             []byte
	Meta            byte
	UserMeta        byte
	Value           []byte
	CASCounterCheck uint64 // If nonzero, we will check if existing casCounter matches.
	Error           error  // Error if any.

	// Fields maintained internally.
	offset     uint32
	casCounter uint64
}

// Encodes e to buf. Returns number of bytes written.
func encodeEntry(e *Entry, buf *bytes.Buffer) (int, error) {
	var h header
	h.klen = uint32(len(e.Key))
	h.vlen = uint32(len(e.Value))
	h.meta = e.Meta
	h.userMeta = e.UserMeta
	h.casCounter = e.casCounter
	h.casCounterCheck = e.CASCounterCheck

	var headerEnc [headerBufSize]byte
	h.Encode(headerEnc[:])

	hash := crc32.New(y.CastagnoliCrcTable)

	buf.Write(headerEnc[:])
	hash.Write(headerEnc[:])

	buf.Write(e.Key)
	hash.Write(e.Key)

	buf.Write(e.Value)
	hash.Write(e.Value)

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	buf.Write(crcBuf[:])

	return len(headerEnc) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d UserMeta: %d Offset: %d len(val)=%d cas=%d check=%d\n",
		prefix, e.Key, e.Meta, e.UserMeta, e.offset, len(e.Value), e.casCounter, e.CASCounterCheck)
}

type header struct {
	klen            uint32
	vlen            uint32
	meta            byte
	userMeta        byte
	casCounter      uint64
	casCounterCheck uint64
}

const (
	headerBufSize = 26
)

func (h header) Encode(out []byte) {
	y.AssertTrue(len(out) >= headerBufSize)
	binary.BigEndian.PutUint32(out[0:4], h.klen)
	binary.BigEndian.PutUint32(out[4:8], h.vlen)
	out[8] = h.meta
	out[9] = h.userMeta
	binary.BigEndian.PutUint64(out[10:18], h.casCounter)
	binary.BigEndian.PutUint64(out[18:26], h.casCounterCheck)
}

// Decodes h from buf.
func (h *header) Decode(buf []byte) {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	h.meta = buf[8]
	h.userMeta = buf[9]
	h.casCounter = binary.BigEndian.Uint64(buf[10:18])
	h.casCounterCheck = binary.BigEndian.Uint64(buf[18:26])
}

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

func (p valuePointer) Less(o valuePointer) bool {
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p valuePointer) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

const valuePointerEncodedSize = 12

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint32(b[8:valuePointerEncodedSize], p.Offset)
	return b[:valuePointerEncodedSize]
}

func (p *valuePointer) Decode(b []byte) {
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint32(b[8:valuePointerEncodedSize])
}

type valueLog struct {
	buf     bytes.Buffer
	dirPath string
	elog    trace.EventLog

	// guards our view of which files exist, which to be deleted, how many active iterators
	filesLock        sync.RWMutex
	filesMap         map[uint32]*logFile
	filesToBeDeleted []uint32
	// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
	numActiveIterators int

	kv                *KV
	maxFid            uint32
	writableLogOffset uint32
	opt               Options

	garbageCh chan struct{}
}

func vlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.vlog", dirPath, string(os.PathSeparator), fid)
}

func (vlog *valueLog) fpath(fid uint32) string {
	return vlogFilePath(vlog.dirPath, fid)
}

func (vlog *valueLog) openOrCreateFiles() error {
	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return errors.Wrapf(err, "Error while opening value log")
	}

	found := make(map[uint64]struct{})
	var maxFid uint32 // Beware len(files) == 0 case, this starts at 0.
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-5], 10, 32)
		if err != nil {
			return errors.Wrapf(err, "Error while parsing value log id for file: %q", file.Name())
		}
		if _, ok := found[fid]; ok {
			return errors.Errorf("Found the same value log file twice: %d", fid)
		}
		found[fid] = struct{}{}

		lf := &logFile{fid: uint32(fid), path: vlog.fpath(uint32(fid))}
		vlog.filesMap[uint32(fid)] = lf
		if uint32(fid) > maxFid {
			maxFid = uint32(fid)
		}
	}
	vlog.maxFid = uint32(maxFid)

	// Open all previous log files as read only. Open the last log file
	// as read write.
	for fid, lf := range vlog.filesMap {
		if fid == maxFid {
			if lf.fd, err = y.OpenExistingSyncedFile(vlog.fpath(fid),
				vlog.opt.SyncWrites); err != nil {
				return errors.Wrapf(err, "Unable to open value log file as RDWR")
			}
		} else {
			if err := lf.openReadOnly(); err != nil {
				return err
			}
		}
	}

	// If no files are found, then create a new file.
	if len(vlog.filesMap) == 0 {
		// We already set vlog.maxFid above
		_, err := vlog.createVlogFile(0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) createVlogFile(fid uint32) (*logFile, error) {
	path := vlog.fpath(fid)
	lf := &logFile{fid: fid, path: path}
	vlog.writableLogOffset = 0

	var err error
	if lf.fd, err = y.CreateSyncedFile(path, vlog.opt.SyncWrites); err != nil {
		return nil, errors.Wrapf(err, "Unable to create value log file")
	}

	if err = syncDir(vlog.dirPath); err != nil {
		return nil, errors.Wrapf(err, "Unable to sync value log file dir")
	}

	vlog.filesLock.Lock()
	vlog.filesMap[fid] = lf
	vlog.filesLock.Unlock()

	return lf, nil
}

func (vlog *valueLog) Open(kv *KV, opt *Options) error {
	vlog.dirPath = opt.ValueDir
	vlog.opt = *opt
	vlog.kv = kv
	vlog.filesMap = make(map[uint32]*logFile)
	if err := vlog.openOrCreateFiles(); err != nil {
		return errors.Wrapf(err, "Unable to open value log")
	}

	vlog.elog = trace.NewEventLog("Badger", "Valuelog")
	vlog.garbageCh = make(chan struct{}, 1) // Only allow one GC at a time.

	return nil
}

func (vlog *valueLog) Close() error {
	vlog.elog.Printf("Stopping garbage collection of values.")
	defer vlog.elog.Finish()

	var err error
	for id, f := range vlog.filesMap {

		f.lock.Lock() // We won’t release the lock.
		if munmapErr := y.Munmap(f.fmap); munmapErr != nil && err == nil {
			err = munmapErr
		}

		if id == vlog.maxFid {
			// truncate writable log file to correct offset.
			if truncErr := f.fd.Truncate(
				int64(vlog.writableLogOffset)); truncErr != nil && err == nil {
				err = truncErr
			}
		}

		if closeErr := f.fd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}

	}
	return err
}

// sortedFids returns the file id's not pending deletion, sorted.  Assumes we have shared access to
// filesMap.
func (vlog *valueLog) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range vlog.filesToBeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	ret := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if _, ok := toBeDeleted[fid]; !ok {
			ret = append(ret, fid)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

// Replay replays the value log. The kv provided is only valid for the lifetime of function call.
func (vlog *valueLog) Replay(ptr valuePointer, fn logEntry) error {
	fid := ptr.Fid
	offset := ptr.Offset + ptr.Len
	vlog.elog.Printf("Seeking at value pointer: %+v\n", ptr)

	fids := vlog.sortedFids()

	for _, id := range fids {
		if id < fid {
			continue
		}
		of := offset
		if id > fid {
			of = 0
		}
		f := vlog.filesMap[id]
		err := vlog.iterate(f, of, fn)
		if err != nil {
			return errors.Wrapf(err, "Unable to replay value log: %q", f.path)
		}
	}

	// Seek to the end to start writing.
	var err error
	last := vlog.filesMap[vlog.maxFid]
	lastOffset, err := last.fd.Seek(0, io.SeekEnd)
	atomic.AddUint32(&vlog.writableLogOffset, uint32(lastOffset))
	return errors.Wrapf(err, "Unable to seek to end of value log: %q", last.path)
}

type request struct {
	// Input values
	Entries []*Entry
	// Output values and wait group stuff below
	Ptrs []valuePointer
	Wg   sync.WaitGroup
	Err  error
}

// sync is thread-unsafe and should not be called concurrently with write.
func (vlog *valueLog) sync() error {
	if vlog.opt.SyncWrites {
		return nil
	}

	vlog.filesLock.RLock()
	if len(vlog.filesMap) == 0 {
		vlog.filesLock.RUnlock()
		return nil
	}
	curlf := vlog.filesMap[vlog.maxFid]
	curlf.lock.RLock()
	vlog.filesLock.RUnlock()

	dirSyncCh := make(chan error)
	go func() { dirSyncCh <- syncDir(vlog.opt.ValueDir) }()
	err := curlf.sync()
	curlf.lock.RUnlock()
	dirSyncErr := <-dirSyncCh
	if err != nil {
		err = dirSyncErr
	}
	return err
}

func (vlog *valueLog) writableOffset() uint32 {
	return atomic.LoadUint32(&vlog.writableLogOffset)
}

// write is thread-unsafe by design and should not be called concurrently.
func (vlog *valueLog) write(reqs []*request) error {
	vlog.filesLock.RLock()
	curlf := vlog.filesMap[vlog.maxFid]
	vlog.filesLock.RUnlock()

	toDisk := func() error {
		if vlog.buf.Len() == 0 {
			return nil
		}
		vlog.elog.Printf("Flushing %d blocks of total size: %d", len(reqs), vlog.buf.Len())
		n, err := curlf.fd.Write(vlog.buf.Bytes())
		if err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", curlf.path)
		}
		y.NumWrites.Add(1)
		y.NumBytesWritten.Add(int64(n))
		vlog.elog.Printf("Done")
		atomic.AddUint32(&vlog.writableLogOffset, uint32(n))
		vlog.buf.Reset()

		if vlog.writableOffset() > uint32(vlog.opt.ValueLogFileSize) {
			var err error
			if err = curlf.doneWriting(vlog.writableLogOffset); err != nil {
				return err
			}

			newid := atomic.AddUint32(&vlog.maxFid, 1)
			y.AssertTruef(newid < 1<<16, "newid will overflow uint16: %v", newid)
			newlf, err := vlog.createVlogFile(newid)
			if err != nil {
				return err
			}

			if err = newlf.mmap(2 * vlog.opt.ValueLogFileSize); err != nil {
				return err
			}

			curlf = newlf
		}
		return nil
	}

	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]
			var p valuePointer

			if !vlog.opt.SyncWrites && len(e.Value) < vlog.opt.ValueThreshold {
				// No need to write to value log.
				b.Ptrs = append(b.Ptrs, p)
				continue
			}

			p.Fid = curlf.fid
			// Use the offset including buffer length so far.
			p.Offset = vlog.writableOffset() + uint32(vlog.buf.Len())
			plen, err := encodeEntry(e, &vlog.buf) // Now encode the entry into buffer.
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			b.Ptrs = append(b.Ptrs, p)

			if p.Offset > uint32(vlog.opt.ValueLogFileSize) {
				if err := toDisk(); err != nil {
					return err
				}
			}
		}
	}
	return toDisk()

	// Acquire mutex locks around this manipulation, so that the reads don't try to use
	// an invalid file descriptor.
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (vlog *valueLog) getFileRLocked(fid uint32) (*logFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	ret, ok := vlog.filesMap[fid]
	if !ok {
		// log file has gone away, will need to retry the operation.
		return nil, ErrRetry
	}
	ret.lock.RLock()
	return ret, nil
}

// Read reads the value log at a given location.
func (vlog *valueLog) Read(vp valuePointer, consumer func([]byte) error) error {
	// Check for valid offset if we are reading to writable log.
	if vp.Fid == vlog.maxFid && vp.Offset >= vlog.writableOffset() {
		return errors.Errorf(
			"Invalid value pointer offset: %d greater than current offset: %d",
			vp.Offset, vlog.writableOffset())
	}

	fn := func(buf []byte) error {
		var h header
		h.Decode(buf)
		if (h.meta & BitDelete) != 0 {
			// Tombstone key
			return consumer(nil)
		}
		n := uint32(headerBufSize)
		n += h.klen
		return consumer(buf[n : n+h.vlen])
	}
	return vlog.readValueBytes(vp, fn)
}

func (vlog *valueLog) readValueBytes(vp valuePointer, consumer func([]byte) error) error {
	lf, err := vlog.getFileRLocked(vp.Fid)
	if err != nil {
		return errors.Wrapf(err, "Unable to read from value log: %+v", vp)
	}
	defer lf.lock.RUnlock()

	var buf []byte
	if buf, err = lf.read(vp); err != nil {
		return errors.Wrapf(err, "Unable to read from value log: %+v", vp)
	}
	return consumer(buf)
}

// Test helper
func valueBytesToEntry(buf []byte) (e Entry) {
	var h header
	h.Decode(buf)
	n := uint32(headerBufSize)

	e.Key = buf[n : n+h.klen]
	n += h.klen
	e.Meta = h.meta
	e.UserMeta = h.userMeta
	e.casCounter = h.casCounter
	e.CASCounterCheck = h.casCounterCheck
	e.Value = buf[n : n+h.vlen]
	return
}

func (vlog *valueLog) pickLog() *logFile {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	fids := vlog.sortedFids()
	if len(fids) <= 1 {
		return nil
	}
	// This file shouldn't be being written to.
	idx := rand.Intn(len(fids))
	if idx > 0 {
		idx = rand.Intn(idx) // Another level of rand to favor smaller fids.
	}
	return vlog.filesMap[fids[idx]]
}

func (vlog *valueLog) doRunGC(gcThreshold float64) error {
	lf := vlog.pickLog()
	if lf == nil {
		return ErrNoRewrite
	}

	type reason struct {
		total   float64
		keep    float64
		discard float64
	}

	var r reason
	var window = 100.0
	count := 0

	// Pick a random start point for the log.
	skipFirstM := float64(rand.Intn(int(vlog.opt.ValueLogFileSize/M))) - window
	var skipped float64

	start := time.Now()
	y.AssertTrue(vlog.kv != nil)
	err := vlog.iterate(lf, 0, func(e Entry, vp valuePointer) error {
		esz := float64(vp.Len) / (1 << 20) // in MBs. +4 for the CAS stuff.
		skipped += esz
		if skipped < skipFirstM {
			return nil
		}

		count++
		if count%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		r.total += esz
		if r.total > window {
			return errStop
		}
		if time.Since(start) > 10*time.Second {
			return errStop
		}

		vs, err := vlog.kv.get(e.Key)
		if err != nil {
			return err
		}
		if (vs.Meta & BitDelete) > 0 {
			// Key has been deleted. Discard.
			r.discard += esz
			return nil
		}
		if (vs.Meta & BitValuePointer) == 0 {
			// Value is stored alongside key. Discard.
			r.discard += esz
			return nil
		}

		// Value is still present in value log.
		y.AssertTrue(len(vs.Value) > 0)

		vp.Decode(vs.Value)

		if vp.Fid > lf.fid {
			// Value is present in a later log. Discard.
			r.discard += esz
			return nil
		}
		if vp.Offset > e.offset {
			// Value is present in a later offset, but in the same log.
			r.discard += esz
			return nil
		}
		if vp.Fid == lf.fid && vp.Offset == e.offset {
			// This is still the active entry. This would need to be rewritten.
			r.keep += esz

		} else {
			vlog.elog.Printf("Reason=%+v\n", r)

			err := vlog.readValueBytes(vp, func(buf []byte) error {
				ne := valueBytesToEntry(buf)
				ne.offset = vp.Offset
				if ne.casCounter == e.casCounter {
					ne.print("Latest Entry Header in LSM")
					e.print("Latest Entry in Log")
					return errors.Errorf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.",
						vp, vs.Meta)
				}
				return nil
			})
			if err != nil {
				return errStop
			}
		}
		return nil
	})

	if err != nil {
		vlog.elog.Errorf("Error while iterating for RunGC: %v", err)
		return err
	}
	vlog.elog.Printf("Fid: %d Data status=%+v\n", lf.fid, r)

	if r.total < 10.0 || r.discard < gcThreshold*r.total {
		vlog.elog.Printf("Skipping GC on fid: %d\n\n", lf.fid)
		return ErrNoRewrite
	}

	vlog.elog.Printf("REWRITING VLOG %d\n", lf.fid)
	if err = vlog.rewrite(lf); err != nil {
		return err
	}
	vlog.elog.Printf("Done rewriting.")
	return nil
}

func (vlog *valueLog) waitOnGC(lc *y.Closer) {
	defer lc.Done()

	<-lc.HasBeenClosed() // Wait for lc to be closed.

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	vlog.garbageCh <- struct{}{}
}

func (vlog *valueLog) runGC(gcThreshold float64) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		err := vlog.doRunGC(gcThreshold)
		<-vlog.garbageCh
		return err
	default:
		return ErrRejected
	}
}
