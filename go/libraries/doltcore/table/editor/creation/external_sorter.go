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

package creation

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
	"github.com/dolthub/dolt/go/store/val"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"sort"
	"sync"
)

type tupleSorter struct {
	keyCmp    func(val.Tuple, val.Tuple) bool
	files     []*keyFile
	inProg    *keyMem
	fileMax   int
	batchSize int
	tmpProv   tempfiles.TempFileProvider
}

func newTupleSorter(batchSize, fileMax int, keyCmp func(val.Tuple, val.Tuple) bool) *tupleSorter {
	if fileMax%2 == 1 {
		// round down to even
		// fileMax/2 will be merge parallelism
		fileMax -= 1
	}
	ret := &tupleSorter{
		fileMax:   fileMax,
		batchSize: batchSize,
		keyCmp:    keyCmp,
		tmpProv:   tempfiles.MovableTempFileProvider,
	}
	ret.inProg = newKeyMem(ret.newFile(), batchSize)
	return ret
}

func (a *tupleSorter) flush(ctx context.Context) *keyFile {
	// k-way merge sort
	allKeys := newKeyFile(a.newFile(), a.inProg.byteLim)

	// don't flush in-progress, just sort in memory
	a.inProg.sort(a.keyCmp)
	var iterables []keyIterable

	iterables = append(iterables, a.inProg)
	for _, file := range a.files {
		iterables = append(iterables, file)
	}
	m := a.newFileMerger(ctx, allKeys, iterables...)
	m.run(ctx)
	return allKeys
}

func (a *tupleSorter) insert(ctx context.Context, k val.Tuple) {
	// add to ongoing file
	if !a.inProg.insert(k) {
		a.flushMem(ctx)
		a.inProg.insert(k)
	}
	return
}

func (a *tupleSorter) flushMem(ctx context.Context) {
	// flush and replace |inProg|
	newFile := a.inProg.flush(a.keyCmp)
	a.inProg = newKeyMem(a.newFile(), a.batchSize)
	a.files = append(a.files, newFile)
	if len(a.files) >= a.fileMax {
		// merge sort files
		a.compact(ctx)
	}
}

func (a *tupleSorter) newFile() *os.File {
	f, err := a.tmpProv.NewFile("", "key_file_")
	if err != nil {
		newError(err)
	}
	return f
}

func (a *tupleSorter) compact(ctx context.Context) {
	// write keyFiles into half as many with double length
	var newFiles []*keyFile
	wg := sync.WaitGroup{}
	eg, ctx := errgroup.WithContext(ctx)
	work := make(chan *keyFile)
	eg.Go(func() error {
		for {
			select {
			case newF, ok := <-work:
				if !ok {
					return nil
				}
				newFiles = append(newFiles, newF)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	for i := 0; i < len(a.files); i += 2 {
		i := i
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			outF := newKeyFile(a.newFile(), a.files[i].batchSize)
			m := a.newFileMerger(ctx, outF, a.files[i], a.files[i+1])
			m.run(ctx)
			work <- outF
			return nil
		})
	}
	wg.Wait()
	close(work)
	eg.Wait()
	a.files = newFiles
}

func newKeyMem(f *os.File, size int) *keyMem {
	return &keyMem{f: f, byteLim: size}
}

type keyMem struct {
	keys    []val.Tuple
	f       *os.File
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

func (f *keyMem) iterAll(_ context.Context) keyIter {
	return &keyMemIter{keys: f.keys}
}

type keyMemIter struct {
	keys []val.Tuple
	i    int
}

func (i *keyMemIter) next(_ context.Context) (val.Tuple, bool) {
	if i.i >= len(i.keys) {
		return nil, false
	}
	ret := i.keys[i.i]
	i.i++
	return ret, true
}

func (i *keyMemIter) close() {
	return
}

func (k *keyMem) Len() int {
	return len(k.keys)
}

func (k *keyMem) sort(cmp func(val.Tuple, val.Tuple) bool) {
	sort.Slice(k.keys, func(i, j int) bool {
		return cmp(k.keys[i], k.keys[j])
	})
}

func (k *keyMem) flush(cmp func(val.Tuple, val.Tuple) bool) *keyFile {
	// size, tuple
	sort.Slice(k.keys, func(i, j int) bool {
		return cmp(k.keys[i], k.keys[j])
	})
	buf := make([]byte, k.byteCnt)
	i := 0
	for _, k := range k.keys {
		writeUint32(buf[i:], uint32(len(k)))
		i += keyLenSz
		copy(buf[i:i+len(k)], k[:])
		i += len(k)
	}
	k.f.Write(buf)
	k.f.Sync()
	return &keyFile{f: k.f, batchSize: k.byteLim}
}

func newKeyFile(f *os.File, batchSize int) *keyFile {
	return &keyFile{f: f, batchSize: batchSize}
}

type keyFile struct {
	f         *os.File
	batchSize int
}

func (f *keyFile) iterAll(ctx context.Context) keyIter {
	eg, ctx := errgroup.WithContext(ctx)
	iter := &keyFileReader{f: f.f, eg: eg, batchSize: f.batchSize, buf: make(chan val.Tuple), closed: make(chan struct{})}
	if f.batchSize == 0 {
		panic("bad batch size")
	}
	eg.Go(func() error {
		return iter.readahead(ctx)
	})
	return iter
}

func (f *keyFile) append(k val.Tuple) {
	sizeBuf := make([]byte, keyLenSz)
	writeUint32(sizeBuf, uint32(len(k)))
	n, err := f.f.Write(sizeBuf)
	if err != nil {
		newError(err)
	}
	if n != len(sizeBuf) {
		newError(fmt.Errorf("short write failure"))
	}
	n, err = f.f.Write(k)
	if err != nil {
		newError(err)
	}
	if n != len(k) {
		newError(fmt.Errorf("short write failure"))
	}
}

type keyFileReader struct {
	f   *os.File
	pos int64

	eg        *errgroup.Group
	batchSize int
	keySize   int

	buf    chan (val.Tuple)
	closed chan (struct{})
	cur    []val.Tuple
	curIdx int
}

func (r *keyFileReader) readahead(ctx context.Context) error {
	defer close(r.buf)
	for {
		buf, more := r.readBatch()
		for _, key := range buf {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.closed:
				return nil
			case r.buf <- key:
			}
		}
		if !more {
			return nil
		}
	}
}

const (
	keyLenSz = 4
)

func (r *keyFileReader) readBatch() (keys []val.Tuple, more bool) {
	// read contiguous segment into buffer
	// split buffer into tuples
	// update file position to include latest complete key
	buf := make([]byte, r.batchSize)
	n, err := r.f.ReadAt(buf, r.pos)
	more = !errors.Is(err, io.EOF)
	if err != nil && more {
		newError(err)
	}
	buf = buf[:n]

	pos := int(r.pos)
	for {
		// | size | tuple | ...
		if keyLenSz > len(buf) {
			break
		}
		keySize := readUint32(buf)
		buf = buf[keyLenSz:]

		if int(keySize) > len(buf) {
			break
		}
		tup := val.Tuple(buf[:keySize])
		buf = buf[keySize:]

		keys = append(keys, tup)
		pos += keyLenSz + int(keySize)
	}

	r.pos = int64(pos)
	return keys, more
}

func readUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint32(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}

func (r *keyFileReader) next(ctx context.Context) (val.Tuple, bool) {
	select {
	case key, ok := <-r.buf:
		if !ok {
			return nil, false
		}
		return key, true
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			newError(err)
		}
	case <-r.closed:
		newError(fmt.Errorf("tried to read from cloesd channel"))
	}
	return nil, false
}

func (r *keyFileReader) close() {
	close(r.closed)
	return
}

type keyIterable interface {
	iterAll(context.Context) keyIter
}

type keyIter interface {
	next(ctx context.Context) (val.Tuple, bool)
	close()
}

type mergeFileReader struct {
	file    keyIter
	root    val.Tuple
	heapIdx int
}

func (r *mergeFileReader) next(ctx context.Context) bool {
	var ok bool
	r.root, ok = r.file.next(ctx)
	return ok
}

func newMergeFileReader(ctx context.Context, iter keyIter, heapIdx int) *mergeFileReader {
	root, ok := iter.next(ctx)
	if !ok {
		// todo close |closed| here?
		return nil
	}
	return &mergeFileReader{file: iter, root: root, heapIdx: heapIdx}
}

type mergeQueue struct {
	files  []*mergeFileReader
	keyCmp func(val.Tuple, val.Tuple) bool
}

func (mq mergeQueue) Len() int { return len(mq.files) }

func (mq mergeQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not highest, priority so we use less than here.
	return mq.keyCmp(mq.files[i].root, mq.files[j].root)
}

func (mq mergeQueue) Swap(i, j int) {
	mq.files[i], mq.files[j] = mq.files[j], mq.files[i]
	mq.files[i].heapIdx = i
	mq.files[j].heapIdx = j
}

func (mq *mergeQueue) Push(x any) {
	n := len(mq.files)
	item := x.(*mergeFileReader)
	item.heapIdx = n
	mq.files = append(mq.files, item)
}

func (mq *mergeQueue) Pop() any {
	old := mq.files
	n := len(old)
	item := old[n-1]
	old[n-1] = nil    // avoid memory leak
	item.heapIdx = -1 // for safety
	mq.files = old[0 : n-1]
	return item
}

type fileMerger struct {
	mq  *mergeQueue
	out *keyFile
}

func (a *tupleSorter) newFileMerger(ctx context.Context, target *keyFile, files ...keyIterable) *fileMerger {
	// get iterators from the keyFiles
	// add to the file heap
	// heapify to get first sorting
	var fileRoots []*mergeFileReader
	for i, f := range files {
		iter := f.iterAll(ctx)
		reader := newMergeFileReader(ctx, iter, i)
		if reader != nil {
			fileRoots = append(fileRoots, reader)
		}
	}

	mq := &mergeQueue{files: fileRoots, keyCmp: a.keyCmp}
	heap.Init(mq)

	return &fileMerger{
		mq:  mq,
		out: target,
	}
}

func (m *fileMerger) run(ctx context.Context) {
	for {
		if m.mq.Len() == 0 {
			m.finalize()
			return
		}
		reader := heap.Pop(m.mq).(*mergeFileReader)
		m.out.append(reader.root)
		if ok := reader.next(ctx); ok {
			heap.Push(m.mq, reader)
		} else {
			defer reader.file.close()
		}
	}
}

func (m *fileMerger) finalize() {
	if err := m.out.f.Sync(); err != nil {
		newError(err)
	}
}

func newError(err error) {
	panic(indexBuildErr{err: err})
}

type indexBuildErr struct {
	err error
}
