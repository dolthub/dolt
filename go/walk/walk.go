// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package walk implements an API for iterating on Noms values.
package walk

import (
	"fmt"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// SomeCallback takes a types.Value and returns a bool indicating whether the current walk should skip the tree descending from value. If |v| is a top-level value in a Chunk, then |r| will be the Ref which referenced it (otherwise |r| is nil).
type SomeCallback func(v types.Value, r *types.Ref) bool

// AllCallback takes a types.Value and processes it. If |v| is a top-level value in a Chunk, then |r| will be the Ref which referenced it (otherwise |r| is nil).
type AllCallback func(v types.Value, r *types.Ref)

// SomeP recursively walks over all types.Values reachable from r and calls cb on them. If cb ever returns true, the walk will stop recursing on the current ref. If |concurrency| > 1, it is the callers responsibility to make ensure that |cb| is threadsafe.
func SomeP(v types.Value, vr types.ValueReader, cb SomeCallback, concurrency int) {
	doTreeWalkP(v, vr, cb, concurrency)
}

// AllP recursively walks over all types.Values reachable from r and calls cb on them. If |concurrency| > 1, it is the callers responsibility to make ensure that |cb| is threadsafe.
func AllP(v types.Value, vr types.ValueReader, cb AllCallback, concurrency int) {
	doTreeWalkP(v, vr, func(v types.Value, r *types.Ref) (skip bool) {
		cb(v, r)
		return
	}, concurrency)
}

func doTreeWalkP(v types.Value, vr types.ValueReader, cb SomeCallback, concurrency int) {
	rq := newRefQueue()
	f := newFailure()

	visited := map[hash.Hash]bool{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	var processVal func(v types.Value, r *types.Ref)
	processVal = func(v types.Value, r *types.Ref) {
		if cb(v, r) {
			return
		}

		if sr, ok := v.(types.Ref); ok {
			wg.Add(1)
			rq.tail() <- sr
		} else {
			switch coll := v.(type) {
			case types.List:
				coll.IterAll(func(c types.Value, index uint64) {
					processVal(c, nil)
				})
			case types.Set:
				coll.IterAll(func(c types.Value) {
					processVal(c, nil)
				})
			case types.Map:
				coll.IterAll(func(k, c types.Value) {
					processVal(k, nil)
					processVal(c, nil)
				})
			default:
				for _, c := range v.ChildValues() {
					processVal(c, nil)
				}
			}
		}
	}

	processRef := func(r types.Ref) {
		defer wg.Done()

		mu.Lock()
		skip := visited[r.TargetHash()]
		visited[r.TargetHash()] = true
		mu.Unlock()

		if skip || f.didFail() {
			return
		}

		target := r.TargetHash()
		v := vr.ReadValue(target)
		if v == nil {
			f.fail(fmt.Errorf("Attempt to visit absent ref:%s", target.String()))
			return
		}
		processVal(v, &r)
	}

	iter := func() {
		for r := range rq.head() {
			processRef(r)
		}
	}

	for i := 0; i < concurrency; i++ {
		go iter()
	}

	processVal(v, nil)
	wg.Wait()

	rq.close()

	f.checkNotFailed()
}

// SomeChunksStopCallback is called for every unique types.Ref |r|. Return true to stop walking beyond |r|.
type SomeChunksStopCallback func(r types.Ref) bool

// SomeChunksChunkCallback is called for every unique chunks.Chunk |c| which wasn't stopped from SomeChunksStopCallback. |r| is a types.Ref referring to |c|.
type SomeChunksChunkCallback func(r types.Ref, c chunks.Chunk)

// SomeChunksP invokes callbacks on every unique chunk reachable from |r| in top-down order. Callbacks are invoked only once for each chunk regardless of how many times the chunk appears.
//
// |stopCb| is invoked for the types.Ref of every chunk. It can return true to stop SomeChunksP from descending any further.
// |chunkCb| is optional, invoked with the chunks.Chunk referenced by |stopCb| if it didn't return true.
func SomeChunksP(r types.Ref, bs types.BatchStore, stopCb SomeChunksStopCallback, chunkCb SomeChunksChunkCallback, concurrency int) {
	rq := newRefQueue()
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	visitedRefs := map[hash.Hash]bool{}

	walkChunk := func(r types.Ref) {
		defer wg.Done()

		tr := r.TargetHash()

		mu.Lock()
		visited := visitedRefs[tr]
		visitedRefs[tr] = true
		mu.Unlock()

		if visited || stopCb(r) {
			return
		}

		// Try to avoid the cost of reading |c|. It's only necessary if the caller wants to know about every chunk, or if we need to descend below |c| (ref height > 1).
		var c chunks.Chunk

		if chunkCb != nil || r.Height() > 1 {
			c = bs.Get(tr)
			d.PanicIfTrue(c.IsEmpty())

			if chunkCb != nil {
				chunkCb(r, c)
			}
		}

		if r.Height() == 1 {
			return
		}

		v := types.DecodeValue(c, nil)
		for _, r1 := range v.Chunks() {
			wg.Add(1)
			rq.tail() <- r1
		}
	}

	iter := func() {
		for r := range rq.head() {
			walkChunk(r)
		}
	}

	for i := 0; i < concurrency; i++ {
		go iter()
	}

	wg.Add(1)
	rq.tail() <- r
	wg.Wait()
	rq.close()
}

// refQueue emulates a buffered channel of refs of unlimited size.
type refQueue struct {
	head  func() <-chan types.Ref
	tail  func() chan<- types.Ref
	close func()
}

func newRefQueue() refQueue {
	head := make(chan types.Ref, 64)
	tail := make(chan types.Ref, 64)
	done := make(chan struct{})
	buff := []types.Ref{}

	push := func(r types.Ref) {
		buff = append(buff, r)
	}

	pop := func() types.Ref {
		d.PanicIfFalse(len(buff) > 0)
		r := buff[0]
		buff = buff[1:]
		return r
	}

	go func() {
	loop:
		for {
			if len(buff) == 0 {
				select {
				case r := <-tail:
					push(r)
				case <-done:
					break loop
				}
			} else {
				first := buff[0]
				select {
				case r := <-tail:
					push(r)
				case head <- first:
					r := pop()
					d.PanicIfFalse(r == first)
				case <-done:
					break loop
				}
			}
		}
	}()

	return refQueue{
		func() <-chan types.Ref {
			return head
		},
		func() chan<- types.Ref {
			return tail
		},
		func() {
			close(head)
			done <- struct{}{}
		},
	}
}

type failure struct {
	err error
	mu  *sync.Mutex
}

func newFailure() *failure {
	return &failure{
		mu: &sync.Mutex{},
	}
}

func (f *failure) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil { // only capture first error
		f.err = err
	}
}

func (f *failure) didFail() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err != nil
}

func (f *failure) checkNotFailed() {
	f.mu.Lock()
	defer f.mu.Unlock()
	d.Chk.NoError(f.err)
}
