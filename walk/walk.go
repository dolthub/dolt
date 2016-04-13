package walk

import (
	"fmt"
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// SomeCallback takes a types.Value and returns a bool indicating whether the current walk should skip the tree descending from value. If |v| is a top-level value in a Chunk, then |r| will be the Ref which referenced it (otherwise |r| is nil).
type SomeCallback func(v types.Value, r types.RefBase) bool

// AllCallback takes a types.Value and processes it. If |v| is a top-level value in a Chunk, then |r| will be the Ref which referenced it (otherwise |r| is nil).
type AllCallback func(v types.Value, r types.RefBase)

// SomeP recursively walks over all types.Values reachable from r and calls cb on them. If cb ever returns true, the walk will stop recursing on the current ref. If |concurrency| > 1, it is the callers responsibility to make ensure that |cb| is threadsafe.
func SomeP(v types.Value, vr types.ValueReader, cb SomeCallback, concurrency int) {
	doTreeWalkP(v, vr, cb, concurrency)
}

// AllP recursively walks over all types.Values reachable from r and calls cb on them. If |concurrency| > 1, it is the callers responsibility to make ensure that |cb| is threadsafe.
func AllP(v types.Value, vr types.ValueReader, cb AllCallback, concurrency int) {
	doTreeWalkP(v, vr, func(v types.Value, r types.RefBase) (skip bool) {
		cb(v, r)
		return
	}, concurrency)
}

func doTreeWalkP(v types.Value, vr types.ValueReader, cb SomeCallback, concurrency int) {
	rq := newRefQueue()
	f := newFailure()

	visited := map[ref.Ref]bool{}
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	var processVal func(v types.Value, r types.RefBase)
	processVal = func(v types.Value, r types.RefBase) {
		if cb(v, r) {
			return
		}

		if sr, ok := v.(types.RefBase); ok {
			wg.Add(1)
			rq.tail() <- sr
		} else {
			for _, c := range v.ChildValues() {
				processVal(c, nil)
			}
		}
	}

	processRef := func(r types.RefBase) {
		defer wg.Done()

		mu.Lock()
		skip := visited[r.TargetRef()]
		visited[r.TargetRef()] = true
		mu.Unlock()

		if skip || f.didFail() {
			return
		}

		target := r.TargetRef()
		v := vr.ReadValue(target)
		if v == nil {
			f.fail(fmt.Errorf("Attempt to copy absent ref:%s", target.String()))
			return
		}
		processVal(v, r)
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

// SomeChunksCallback takes a types.RefBase and returns a bool indicating whether
// the current walk should skip the tree descending from value.
type SomeChunksCallback func(r types.RefBase) bool

// SomeChunksP Invokes callback on all chunks reachable from |r| in top-down order. |callback| is invoked only once for each chunk regardless of how many times the chunk appears
func SomeChunksP(r types.RefBase, vr types.ValueReader, callback SomeChunksCallback, concurrency int) {
	doChunkWalkP(r, vr, callback, concurrency)
}

func doChunkWalkP(r types.RefBase, vr types.ValueReader, callback SomeChunksCallback, concurrency int) {
	rq := newRefQueue()
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	visitedRefs := map[ref.Ref]bool{}

	walkChunk := func(r types.RefBase) {
		defer wg.Done()

		mu.Lock()
		visited := visitedRefs[r.TargetRef()]
		visitedRefs[r.TargetRef()] = true
		mu.Unlock()

		if visited || callback(r) {
			return
		}

		v := vr.ReadValue(r.TargetRef())
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
	head  func() <-chan types.RefBase
	tail  func() chan<- types.RefBase
	close func()
}

func newRefQueue() refQueue {
	head := make(chan types.RefBase, 64)
	tail := make(chan types.RefBase, 64)
	done := make(chan struct{})
	buff := []types.RefBase{}

	push := func(r types.RefBase) {
		buff = append(buff, r)
	}

	pop := func() types.RefBase {
		d.Chk.True(len(buff) > 0)
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
					d.Chk.Equal(r, first)
				case <-done:
					break loop
				}
			}
		}
	}()

	return refQueue{
		func() <-chan types.RefBase {
			return head
		},
		func() chan<- types.RefBase {
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
