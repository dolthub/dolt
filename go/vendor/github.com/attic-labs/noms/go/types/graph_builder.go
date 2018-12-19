// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// GraphBuilder allows non-RAM-bound construction of a graph of nested Maps whose
// leaf collections can be Lists, Sets, or Maps that contain any type of Noms
// Values.
//
// Graphs are built by calling one of the GraphBuilder functions:
//     MapSet(graphKeys, key, value)
//     SetInsert(graphKeys, value)
//     ListAppend(graphKeys, value)
//
// GraphBuilder uses an opCache to store graph operations in leveldb and to be
// able to read them out later in a way which ensures a total ordering of all
// the nodes at each level of the graph. (See opcache.go for more info on how
// that is done)
//
// GraphBuilder.Build() does the work of assembling the graph. Build() gets an
// iterator for this graph from the opCache and uses it to iterate over all the
// operations that have been stored for this graph. opCache ensures that the
// operations are returned in optimal sorted order so that sequenceChunker can
// most efficiently assemble the graph. Build() will ensure that there is a Map
// object for each key in |graphKeys|. Any node that falls in the middle of the
// graph must be a Map, although, intermediate nodes may have any element as keys
// as long as the path formed by the graphKeys doesn't conflict.
//
// MapSet(), SetInsert(), and ListAppend() are threadsafe meaning they can safely
// be called from different go routines. However, the semantics of ListAppend()
// are such that the order of the list will be determined by which thread() calls
// ListAppend first (this function call may be modified later to allow specification
// of index or order).
//
// Build() should only be called once, after all the operations for the graph
// have been stored. It is the caller's responsibility to make sure that all
// calls to the mutation operations have completed before Build() is invoked.
//

package types

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/attic-labs/noms/go/d"
)

type GraphBuilder struct {
	opcStore opCacheStore
	oc       opCache
	vrw      ValueReadWriter
	stack    graphStack
	mutex    sync.Mutex
}

// NewGraphBuilder returns an new GraphBuilder object.
func NewGraphBuilder(vrw ValueReadWriter, rootKind NomsKind) *GraphBuilder {
	return newGraphBuilder(vrw, newLdbOpCacheStore(vrw), rootKind)
}

func newGraphBuilder(vrw ValueReadWriter, opcStore opCacheStore, rootKind NomsKind) *GraphBuilder {
	b := &GraphBuilder{oc: opcStore.opCache(), opcStore: opcStore, vrw: vrw}
	b.pushNewKeyOnStack(String("ROOT"), rootKind)
	return b
}

// MapSet will add the key/value pair |k, v| to the map found by traversing
// the graph using the |keys| slice. Intermediate maps referenced by |keys| are
// created as necessary. This is threadsafe, may call from multiple go routines.
func (b *GraphBuilder) MapSet(keys []Value, k Value, v Value) {
	if b.oc == nil {
		d.Panic("Can't call MapSet() again after Build()")
	}
	b.oc.GraphMapSet(keys, k, v)
}

// SetInsert will insert the value |v| into the set at path |keys|. Intermediate
// maps referenced by |keys| are created as necessary. This is threadsafe, may
// call from multiple go routines.
func (b *GraphBuilder) SetInsert(keys []Value, v Value) {
	if b.oc == nil {
		d.Panic("Can't call SetInsert() again after Build()")
	}
	b.oc.GraphSetInsert(keys, v)
}

// ListAppend will append |v| to the list at path |p|. Intermediate
// maps referenced by |keys| are created as necessary. This is threadsafe, may
// call from multiple go routines, however append semantics are such that the
// elements will be appended in order that functions are called, so order has
// to be managed by caller.
func (b *GraphBuilder) ListAppend(keys []Value, v Value) {
	if b.oc == nil {
		d.Panic("Can't call ListAppend() again after Build()")
	}
	b.oc.GraphListAppend(keys, v)
}

type graphOpContainer struct {
	keys []Value
	kind NomsKind
	item sequenceItem
}

// Build builds and returns the graph. This method should only be called after all
// calls to the mutation operations (i.e. MapSet, SetInsert, and ListAppend)
// have completed. It is the caller's responsibility to ensure that this is
// the case. Build() will panic if called more than once on any GraphBuilder
// object.
func (b *GraphBuilder) Build() Value {
	var opc opCache
	var opcStore opCacheStore

	defer func() {
		opcStore.destroy()
	}()

	// Use function here to take advantage fo the deferred call to mutex.Unlock()
	func() {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		if b.oc == nil {
			d.Panic("Can only call Build() once")
		}
		opcStore, opc = b.opcStore, b.oc
		b.opcStore, b.oc = nil, nil
	}()

	iter := opc.NewIterator()
	defer iter.Release()

	// start up a go routine that will do the reading from graphBuilder's private
	// ldb opCache.
	graphOpChan := make(chan graphOpContainer, 512)
	go func() {
		for iter.Next() {
			keys, kind, item := iter.GraphOp()
			container := graphOpContainer{keys: keys, kind: kind, item: item}
			graphOpChan <- container
		}
		close(graphOpChan)
	}()

	// iterator returns keys, in sort order by array
	for goc := range graphOpChan {
		keys, kind, item := goc.keys, goc.kind, goc.item

		// Get index of first key that is different than what is on the stack
		idx := commonPrefixCount(b.stack, keys)
		if idx == -1 {
			// no keys have changed we're working on same coll as previous
			// iteration, just append to sequenceChunker at top of stack
			b.appendItemToCurrentTopOfStack(kind, item)
			continue
		}

		// Some keys that were in the last graphOp are no longer present
		// which indicates that we are finished adding to those cols. Pop
		// those keys from the stack. This will cause any popped cols to be
		// closed and added to their parents.
		for idx < b.stack.lastIdx() {
			b.popKeyFromStack()
		}

		// We may have popped some keys off of the stack and are left with
		// an item to append to the stack of a previously existing key.
		if b.stack.lastIdx() == len(keys) {
			b.appendItemToCurrentTopOfStack(kind, item)
		}

		// Or we may have some new keys to add to the stack. Add those keys
		// and then append the item to the top element.
		for b.stack.lastIdx() < len(keys) {
			if b.stack.lastIdx() < len(keys)-1 {
				b.pushNewKeyOnStack(keys[b.stack.lastIdx()], MapKind)
			} else {
				b.pushNewKeyOnStack(keys[b.stack.lastIdx()], kind)
				b.appendItemToCurrentTopOfStack(kind, item)
			}
		}
	}

	// We're done adding elements. Pop any intermediate keys off the stack and
	// fold their results into their parent map.
	for b.stack.len() > 1 {
		b.popKeyFromStack()
	}
	res := b.stack.pop().done()
	return res

}

// pushNewKeyOnStack() creates a new graphStackElem node and pushes it on the
// stack. The new element contains the |key| and a new sequenceChunker that will
// be appended to to build this node in the graph.
func (b *GraphBuilder) pushNewKeyOnStack(key Value, kind NomsKind) {
	var ch *sequenceChunker
	switch kind {
	case MapKind:
		ch = newEmptyMapSequenceChunker(b.vrw)
	case SetKind:
		ch = newEmptySetSequenceChunker(b.vrw)
	case ListKind:
		ch = newEmptyListSequenceChunker(b.vrw)
	default:
		panic("bad 'kind' value in GraphBuilder, newElem()")
	}
	b.stack.push(&graphStackElem{key: key, kind: kind, ch: ch})
}

// popKeyFromStack() pops the last element off the stack, calls done() to
// finish any sequenceChunking that is in progress, and then assigns the
// finished collection it's parent map.
func (b *GraphBuilder) popKeyFromStack() {
	elem := b.stack.pop()
	col := elem.done()
	top := b.stack.top()
	top.ch.Append(mapEntry{elem.key, col})
}

// appendItemToCurrentTopOfStack() adds the current item to the sequenceChunker
// that's on the top of the stack.
func (b *GraphBuilder) appendItemToCurrentTopOfStack(kind NomsKind, item sequenceItem) {
	top := b.stack.top()
	d.PanicIfTrue(top.kind != kind)
	top.ch.Append(item)
}

type graphStackElem struct {
	key  Value
	kind NomsKind
	ch   *sequenceChunker
}

type graphStack struct {
	elems []*graphStackElem
}

func (s *graphStack) push(e *graphStackElem) {
	s.elems = append(s.elems, e)
}

func (s *graphStack) pop() *graphStackElem {
	l := len(s.elems) - 1
	elem := s.elems[l]    // last element
	s.elems = s.elems[:l] // truncate last element
	return elem
}

func (s *graphStack) top() *graphStackElem {
	l := len(s.elems) - 1
	return s.elems[l] // last element
}

func (s *graphStack) len() int {
	return len(s.elems)
}

func (s *graphStack) lastIdx() int {
	return len(s.elems) - 1
}

func (s graphStack) String() string {
	buf := bytes.Buffer{}
	for i := len(s.elems) - 1; i >= 0; i-- {
		fmt.Fprintln(&buf, "#:", i, s.elems[i])
	}
	return buf.String()
}

// done() creates the appropriate collection for this element and returns it
func (e *graphStackElem) done() Collection {
	switch e.kind {
	case MapKind:
		return newMap(e.ch.Done().(orderedSequence))
	case SetKind:
		return newSet(e.ch.Done().(orderedSequence))
	case ListKind:
		return newList(e.ch.Done())
	}
	panic("unreachable")
}

// Returns index of first element in keys that is different from stack. Note,
// return value can be equal to len(keys) if there are more element in stack
// than in keys
func commonPrefixCount(stack graphStack, keys ValueSlice) int {
	minLen := len(keys)
	// don't consider the 'ROOT' stack element
	elems := stack.elems[1:]
	if len(elems) < minLen {
		minLen = len(elems)
	}

	for i := 0; i < minLen; i++ {
		if !elems[i].key.Equals(keys[i]) {
			return i
		}
	}

	if len(keys) == len(elems) {
		return -1
	}
	return minLen
}

func (e *graphStackElem) String() string {
	return fmt.Sprintf("key: %s, kind: %s, seq: %p", EncodedValue(e.key), e.kind, e.ch)
}
