// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sort

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dolthub/dolt/go/store/util/tempfiles"
	"github.com/dolthub/dolt/go/store/val"
)

// tupleSorter inputs a series of unsorted tuples and outputs a sorted list
// of tuples. Batches of tuples sorted in memory are written to disk, and
// then k-way merge sorted to produce a final sorted list. The |fileMax|
// parameter limits the number of files spilled to disk at any given time.
// The maximum memory used will be |fileMax| * |batchSize|.
type tupleSorter struct {
	tmpProv   tempfiles.TempFileProvider
	keyCmp    func(val.Tuple, val.Tuple) bool
	inProg    *keyMem
	files     [][]keyIterable
	fileMax   int
	fileCnt   int
	batchSize int
}

func NewTupleSorter(batchSize, fileMax int, keyCmp func(val.Tuple, val.Tuple) bool, tmpProv tempfiles.TempFileProvider) *tupleSorter {
	if fileMax%2 == 1 {
		// round down to even
		// fileMax/2 will be compact parallelism
		fileMax -= 1
	}
	ret := &tupleSorter{
		fileMax:   fileMax,
		batchSize: batchSize,
		keyCmp:    keyCmp,
		tmpProv:   tmpProv,
	}
	ret.inProg = newKeyMem(batchSize)
	return ret
}

func (a *tupleSorter) Flush(ctx context.Context) (iter keyIterable, err error) {
	// don't flush in-progress, just sort in memory
	a.inProg.sort(a.keyCmp)

	if len(a.files) == 0 {
		// don't go to disk if we didn't reach a mem flush
		return a.inProg, nil
	}

	var iterables []keyIterable
	iterables = append(iterables, a.inProg)
	for _, level := range a.files {
		for _, file := range level {
			iterables = append(iterables, file)
		}
	}

	newF, err := a.newFile()
	if err != nil {
		return nil, err
	}
	allKeys := newKeyFile(newF, a.inProg.byteLim)
	defer func() {
		if err != nil {
			allKeys.Close()
		}
	}()

	m, err := newFileMerger(ctx, a.keyCmp, allKeys, iterables...)
	if err != nil {
		return nil, err
	}
	defer m.Close()
	if err := m.run(ctx); err != nil {
		return nil, err
	}
	return allKeys, nil
}

func (a *tupleSorter) Insert(ctx context.Context, k val.Tuple) (err error) {
	if !a.inProg.insert(k) {
		if err := a.flushMem(ctx); err != nil {
			return err
		}
		a.inProg.insert(k)
	}
	return
}
func (a *tupleSorter) Close() {
	for _, level := range a.files {
		for _, f := range level {
			f.Close()
		}
	}
}

func (a *tupleSorter) flushMem(ctx context.Context) error {
	// flush and replace |inProg|
	if a.inProg.Len() > 0 {
		newF, err := a.newFile()
		if err != nil {
			return err
		}
		newFile, err := a.inProg.flush(newF, a.keyCmp)
		if err != nil {
			return err
		}
		a.inProg = newKeyMem(a.batchSize)
		if len(a.files) == 0 {
			a.files = append(a.files, []keyIterable{newFile})
		} else {
			a.files[0] = append(a.files[0], newFile)
		}
		a.fileCnt++
	}
	for level, ok := a.shouldCompact(); ok; level, ok = a.shouldCompact() {
		if err := a.compact(ctx, level); err != nil {
			return err
		}
	}
	return nil
}

func (a *tupleSorter) newFile() (*os.File, error) {
	f, err := a.tmpProv.NewFile("", "key_file_")
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (a *tupleSorter) shouldCompact() (int, bool) {
	for i, level := range a.files {
		if len(level) >= a.fileMax {
			return i, true
		}
	}
	return -1, false
}

// compact merges the first `a.fileMax` files in `a.files[level]` into a single sorted file which is added to `a.files[level+1]`
func (a *tupleSorter) compact(ctx context.Context, level int) error {
	newF, err := a.newFile()
	if err != nil {
		return err
	}
	outF := newKeyFile(newF, a.batchSize)
	func() {
		if err != nil {
			outF.Close()
		}
	}()

	fileLevel := a.files[level]
	m, err := newFileMerger(ctx, a.keyCmp, outF, fileLevel[:a.fileMax]...)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.run(ctx); err != nil {
		return err
	}

	// zero out compacted level
	// add to next level
	a.files[level] = a.files[level][a.fileMax:]
	if len(a.files) <= level+1 {
		a.files = append(a.files, []keyIterable{outF})
	} else {
		a.files[level+1] = append(a.files[level+1], outF)
	}

	return nil
}

func newKeyMem(size int) *keyMem {
	return &keyMem{byteLim: size}
}

type keyMem struct {
	keys    []val.Tuple
	byteCnt int
	byteLim int
}

func (k *keyMem) insert(key val.Tuple) bool {
	if len(key)+int(keyLenSz)+k.byteCnt > k.byteLim {
		return false
	}

	k.keys = append(k.keys, key)
	k.byteCnt += len(key) + int(keyLenSz)
	return true
}

func (k *keyMem) flush(f *os.File, cmp func(val.Tuple, val.Tuple) bool) (*keyFile, error) {
	k.sort(cmp)
	kf := newKeyFile(f, k.byteLim)
	for _, k := range k.keys {
		if err := kf.append(k); err != nil {
			return nil, err
		}
	}
	if err := kf.buf.Flush(); err != nil {
		return nil, err
	}
	return kf, nil
}

func (f *keyMem) IterAll(_ context.Context) (KeyIter, error) {
	return &keyMemIter{keys: f.keys}, nil
}

func (f *keyMem) Close() {
	return
}

type keyMemIter struct {
	keys []val.Tuple
	i    int
}

func (i *keyMemIter) Next(ctx context.Context) (val.Tuple, error) {
	if i.i >= len(i.keys) {
		return nil, io.EOF
	}
	ret := i.keys[i.i]
	i.i++
	return ret, nil
}

func (i *keyMemIter) Close() {
	return
}

func (k *keyMem) Len() int {
	return len(k.keys)
}

// sort sorts the tuples in memory without flushing to disk
func (k *keyMem) sort(cmp func(val.Tuple, val.Tuple) bool) {
	sort.Slice(k.keys, func(i, j int) bool {
		return cmp(k.keys[i], k.keys[j])
	})
}

func newKeyFile(f *os.File, batchSize int) *keyFile {
	return &keyFile{f: f, buf: bufio.NewWriterSize(f, batchSize), batchSize: batchSize}
}

type keyFile struct {
	f         *os.File
	buf       *bufio.Writer
	batchSize int
}

func (f *keyFile) IterAll(ctx context.Context) (KeyIter, error) {
	if f.batchSize == 0 {
		return nil, fmt.Errorf("invalid zero batch size")
	}
	if _, err := f.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	file := f.f
	f.f = nil
	return &keyFileReader{buf: bufio.NewReader(file), f: file}, nil
}

func (f *keyFile) Close() {
	if f != nil && f.f != nil {
		f.f.Close()
		os.Remove(f.f.Name())
	}
	return
}

// append writes |keySize|key| to the intermediate file
func (f *keyFile) append(k val.Tuple) error {
	v := uint32(len(k))
	var sizeBuf [4]byte
	writeUint32(sizeBuf[:], v)

	if _, err := f.buf.Write(sizeBuf[:]); err != nil {
		return err
	}
	if _, err := f.buf.Write(k[:]); err != nil {
		return err
	}

	return nil
}

type keyFileReader struct {
	buf *bufio.Reader
	f   *os.File
}

const (
	keyLenSz = 4
)

func readUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint32(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}

func (r *keyFileReader) Next(ctx context.Context) (val.Tuple, error) {
	var keySizeBuf [4]byte
	if _, err := io.ReadFull(r.buf, keySizeBuf[:]); err != nil {
		return nil, err
	}

	keySize := readUint32(keySizeBuf[:])
	key := make([]byte, keySize)
	if _, err := io.ReadFull(r.buf, key); err != nil {
		return nil, err
	}

	return key, nil
}

func (r *keyFileReader) Close() {
	if r != nil && r.f != nil {
		r.f.Close()
		os.Remove(r.f.Name())
	}
}

type keyIterable interface {
	IterAll(context.Context) (KeyIter, error)
	Close()
}

type KeyIter interface {
	Next(ctx context.Context) (val.Tuple, error)
	Close()
}

// mergeFileReader is the heap object for a k-way merge.
type mergeFileReader struct {
	// iter abstracts file or in-memory sorted tuples
	iter KeyIter
	// head is the next tuple in the sorted list
	head val.Tuple
}

func (r *mergeFileReader) next(ctx context.Context) (bool, error) {
	var err error
	r.head, err = r.iter.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func newMergeFileReader(ctx context.Context, iter KeyIter) (*mergeFileReader, error) {
	root, err := iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	return &mergeFileReader{iter: iter, head: root}, nil
}

type mergeQueue struct {
	keyCmp func(val.Tuple, val.Tuple) bool
	files  []*mergeFileReader
}

func (mq mergeQueue) Len() int { return len(mq.files) }

func (mq mergeQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not highest, priority so we use less than here.
	return mq.keyCmp(mq.files[i].head, mq.files[j].head)
}

func (mq mergeQueue) Swap(i, j int) {
	mq.files[i], mq.files[j] = mq.files[j], mq.files[i]
}

func (mq *mergeQueue) Push(x any) {
	item := x.(*mergeFileReader)
	mq.files = append(mq.files, item)
}

func (mq *mergeQueue) Pop() any {
	old := mq.files
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	mq.files = old[0 : n-1]
	return item
}

type fileMerger struct {
	mq  *mergeQueue
	out *keyFile
}

func newFileMerger(ctx context.Context, keyCmp func(val.Tuple, val.Tuple) bool, target *keyFile, files ...keyIterable) (m *fileMerger, err error) {
	var fileHeads []*mergeFileReader
	defer func() {
		if err != nil {
			for _, fh := range fileHeads {
				fh.iter.Close()
			}
		}
	}()

	for _, f := range files {
		iter, err := f.IterAll(ctx)
		if err != nil {
			return nil, err
		}
		reader, err := newMergeFileReader(ctx, iter)
		if err != nil {
			iter.Close()
			if !errors.Is(err, io.EOF) {
				// empty file excluded from merge queue
				return nil, err
			}
		} else {
			fileHeads = append(fileHeads, reader)
		}
	}

	mq := &mergeQueue{files: fileHeads, keyCmp: keyCmp}
	heap.Init(mq)

	return &fileMerger{
		mq:  mq,
		out: target,
	}, nil
}

func (m *fileMerger) run(ctx context.Context) error {
	for {
		if m.mq.Len() == 0 {
			return m.out.buf.Flush()
		}
		reader := heap.Pop(m.mq).(*mergeFileReader)
		m.out.append(reader.head)
		if ok, err := reader.next(ctx); ok {
			heap.Push(m.mq, reader)
		} else {
			reader.iter.Close()
			if err != nil {
				return err
			}
		}
	}
}

func (m *fileMerger) Close() {
	if m != nil && m.mq != nil {
		for _, f := range m.mq.files {
			f.iter.Close()
		}
	}
}
