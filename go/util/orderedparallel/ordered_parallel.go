// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package orderedparallel

import (
	"container/heap"
	"sync"

	"github.com/attic-labs/noms/go/d"
)

type ProcessFn func(in interface{}) (out interface{})

// Creates a pool of |parallelism| goroutines to process values off of |input| by calling |fn| and guarantees that results of each call will be sent on |out| in the order the corresponding input was received.
func New(input chan interface{}, fn ProcessFn, parallelism int) chan interface{} {
	d.PanicIfFalse(parallelism > 0)

	mu := &sync.Mutex{}
	inCount := uint(0)
	outCount := uint(0)

	wq := make(chan workItem)
	go func() {
		for in := range input {
			wq <- workItem{inCount, in}
			inCount++
		}
		close(wq)
	}()

	outHeap := &workQueue{}
	out := make(chan interface{})

	insertAndProcessHeap := func(item workItem) {
		mu.Lock()
		defer mu.Unlock()

		heap.Push(outHeap, item)
		for outHeap.Peek().order == outCount {
			top := heap.Pop(outHeap).(workItem)
			out <- top.data
			outCount++
		}
	}

	for i := 0; i < parallelism; i++ {
		go func() {
			for item := range wq {
				item.data = fn(item.data)
				insertAndProcessHeap(item)
			}

			mu.Lock()
			if outCount == inCount {
				outCount++ // prevent channel from being closed twice
				close(out)
			}
			mu.Unlock()
		}()
	}

	return out
}

type workItem struct {
	order uint
	data  interface{}
}

type workQueue []workItem

func (wq workQueue) Len() int {
	return len(wq)
}

func (wq workQueue) Less(i, j int) bool {
	return wq[i].order < wq[j].order
}

func (wq workQueue) Swap(i, j int) {
	wq[i], wq[j] = wq[j], wq[i]
}

func (wq *workQueue) Push(r interface{}) {
	*wq = append(*wq, r.(workItem))
}

func (wq *workQueue) Pop() interface{} {
	old := *wq
	n := len(old)
	x := old[n-1]
	*wq = old[0 : n-1]
	return x
}

func (wq workQueue) Empty() bool {
	return len(wq) == 0
}

func (wq workQueue) Peek() (oi workItem) {
	if len(wq) > 0 {
		oi = wq[0]
	}

	return
}
