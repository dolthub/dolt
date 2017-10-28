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
	"bytes"
	"sync"

	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/y"
)

type prefetchStatus uint8

const (
	empty prefetchStatus = iota
	prefetched
)

// KVItem is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type KVItem struct {
	status     prefetchStatus
	err        error
	wg         sync.WaitGroup
	kv         *KV
	key        []byte
	vptr       []byte
	meta       byte
	userMeta   byte
	val        []byte
	casCounter uint64
	slice      *y.Slice
	next       *KVItem
}

// Key returns the key. Remember to copy if you need to access it outside the iteration loop.
func (item *KVItem) Key() []byte {
	return item.key
}

// Value retrieves the value of the item from the value log. It calls the
// consumer function with a slice argument representing the value. In case
// of error, the consumer function is not called.
//
// Note that the call to the consumer func happens synchronously.
//
// Remember to parse or copy it if you need to reuse it. DO NOT modify or
// append to this slice; it would result in a panic.
func (item *KVItem) Value(consumer func([]byte) error) error {
	item.wg.Wait()
	if item.status == prefetched {
		if item.err != nil {
			return item.err
		}
		return consumer(item.val)
	}
	return item.kv.yieldItemValue(item, consumer)
}

func (item *KVItem) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	if (item.meta & BitDelete) != 0 {
		// Tombstone encountered.
		return false
	}
	return true
}

func (item *KVItem) prefetchValue() {
	item.err = item.kv.yieldItemValue(item, func(val []byte) error {
		if val == nil {
			item.status = prefetched
			return nil
		}

		buf := item.slice.Resize(len(val))
		copy(buf, val)
		item.val = buf
		item.status = prefetched
		return nil
	})
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *KVItem) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & BitValuePointer) == 0 {
		return int64(len(item.key) + len(item.vptr))
	}
	var vp valuePointer
	vp.Decode(item.vptr)
	return int64(vp.Len) // includes key length.
}

// Counter returns the CAS counter associated with the value.
func (item *KVItem) Counter() uint64 {
	return item.casCounter
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *KVItem) UserMeta() byte {
	return item.userMeta
}

type list struct {
	head *KVItem
	tail *KVItem
}

func (l *list) push(i *KVItem) {
	i.next = nil
	if l.tail == nil {
		l.head = i
		l.tail = i
		return
	}
	l.tail.next = i
	l.tail = i
}

func (l *list) pop() *KVItem {
	if l.head == nil {
		return nil
	}
	i := l.head
	if l.head == l.tail {
		l.tail = nil
		l.head = nil
	} else {
		l.head = i.next
	}
	i.next = nil
	return i
}

// IteratorOptions is used to set options when iterating over Badger key-value stores.
type IteratorOptions struct {
	// Indicates whether we should prefetch values during iteration and store them.
	PrefetchValues bool
	// How many KV pairs to prefetch while iterating. Valid only if PrefetchValues is true.
	PrefetchSize int
	Reverse      bool // Direction of iteration. False is forward, true is backward.
}

// DefaultIteratorOptions contains default options when iterating over Badger key-value stores.
var DefaultIteratorOptions = IteratorOptions{
	PrefetchValues: true,
	PrefetchSize:   100,
	Reverse:        false,
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	kv   *KV
	iitr *y.MergeIterator

	opt   IteratorOptions
	item  *KVItem
	data  list
	waste list
}

func (it *Iterator) newItem() *KVItem {
	item := it.waste.pop()
	if item == nil {
		item = &KVItem{slice: new(y.Slice), kv: it.kv}
	}
	return item
}

// Item returns pointer to the current KVItem.
// This item is only valid until it.Next() gets called.
func (it *Iterator) Item() *KVItem { return it.item }

// Valid returns false when iteration is done.
func (it *Iterator) Valid() bool { return it.item != nil }

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.item != nil && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	it.iitr.Close()
	// TODO: We could handle this error.
	_ = it.kv.vlog.decrIteratorCount()
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *Iterator) Next() {
	// Reuse current item
	it.item.wg.Wait() // Just cleaner to wait before pushing to avoid doing ref counting.
	it.waste.push(it.item)

	// Set next item to current
	it.item = it.data.pop()

	// Advance internal iterator until entry is not deleted
	for it.iitr.Next(); it.iitr.Valid(); it.iitr.Next() {
		if bytes.HasPrefix(it.iitr.Key(), badgerPrefix) {
			continue
		}
		if it.iitr.Value().Meta&BitDelete == 0 { // Not deleted.
			break
		}
	}

	if !it.iitr.Valid() {
		return
	}
	item := it.newItem()
	it.fill(item)
	it.data.push(item)
}

func (it *Iterator) fill(item *KVItem) {
	vs := it.iitr.Value()
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.casCounter = vs.CASCounter
	item.key = y.Safecopy(item.key, it.iitr.Key())
	item.vptr = y.Safecopy(item.vptr, vs.Value)
	item.val = nil
	if it.opt.PrefetchValues {
		item.wg.Add(1)
		go func() {
			// FIXME we are not handling errors here.
			item.prefetchValue()
			item.wg.Done()
		}()
	}
}

func (it *Iterator) prefetch() {
	prefetchSize := 2
	if it.opt.PrefetchValues && it.opt.PrefetchSize > 1 {
		prefetchSize = it.opt.PrefetchSize
	}

	i := it.iitr
	var count int
	it.item = nil
	for ; i.Valid(); i.Next() {
		if bytes.HasPrefix(it.iitr.Key(), badgerPrefix) {
			continue
		}
		if i.Value().Meta&BitDelete > 0 {
			continue
		}
		count++

		item := it.newItem()
		it.fill(item)
		if it.item == nil {
			it.item = item
		} else {
			it.data.push(item)
		}
		if count == prefetchSize {
			break
		}
	}
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided if iterating in the forward direction. Behavior would be reversed is
// iterating backwards.
func (it *Iterator) Seek(key []byte) {
	for i := it.data.pop(); i != nil; i = it.data.pop() {
		i.wg.Wait()
		it.waste.push(i)
	}
	it.iitr.Seek(key)
	for it.iitr.Valid() && bytes.HasPrefix(it.iitr.Key(), badgerPrefix) {
		it.iitr.Next()
	}
	it.prefetch()
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *Iterator) Rewind() {
	i := it.data.pop()
	for i != nil {
		i.wg.Wait() // Just cleaner to wait before pushing. No ref counting needed.
		it.waste.push(i)
		i = it.data.pop()
	}

	it.iitr.Rewind()
	for it.iitr.Valid() && bytes.HasPrefix(it.iitr.Key(), badgerPrefix) {
		it.iitr.Next()
	}
	it.prefetch()
}

// NewIterator returns a new iterator. Depending upon the options, either only keys, or both
// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
// Usage:
//   opt := badger.DefaultIteratorOptions
//   itr := kv.NewIterator(opt)
//   for itr.Rewind(); itr.Valid(); itr.Next() {
//     item := itr.Item()
//     key := item.Key()
//     var val []byte
//     err = item.Value(func(v []byte) {
//         val = make([]byte, len(v))
// 	       copy(val, v)
//     }) 	// This could block while value is fetched from value log.
//          // For key only iteration, set opt.PrefetchValues to false, and don't call
//          // item.Value(func(v []byte)).
//
//     // Remember that both key, val would become invalid in the next iteration of the loop.
//     // So, if you need access to them outside, copy them or parse them.
//   }
//   itr.Close()
func (s *KV) NewIterator(opt IteratorOptions) *Iterator {
	tables, decr := s.getMemTables()
	defer decr()
	s.vlog.incrIteratorCount()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].NewUniIterator(opt.Reverse))
	}
	iters = s.lc.appendIterators(iters, opt.Reverse) // This will increment references.
	res := &Iterator{
		kv:   s,
		iitr: y.NewMergeIterator(iters, opt.Reverse),
		opt:  opt,
	}
	return res
}
