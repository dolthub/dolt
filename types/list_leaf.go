package types

import (
	"runtime"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type listLeaf struct {
	list []Future
	ref  *ref.Ref
	cs   chunks.ChunkSource
}

func newListLeaf(v ...Value) List {
	return listLeafFromFutures(valuesToFutures(v), nil)
}

func listLeafFromFutures(list []Future, cs chunks.ChunkSource) List {
	return listLeaf{list, &ref.Ref{}, cs}
}

func (l listLeaf) Len() uint64 {
	return uint64(len(l.list))
}

func (l listLeaf) Empty() bool {
	return l.Len() == uint64(0)
}

func (l listLeaf) Get(idx uint64) Value {
	return l.getFuture(idx).Deref(l.cs)
}

func (l listLeaf) Iter(f listIterFunc) {
	for i, fut := range l.list {
		if f(fut.Deref(l.cs), uint64(i)) {
			fut.Release()
			break
		}
		fut.Release()
	}
}

func (l listLeaf) IterAll(f listIterAllFunc) {
	for i, fut := range l.list {
		f(fut.Deref(l.cs), uint64(i))
		fut.Release()
	}
}

func (l listLeaf) Map(mf MapFunc) []interface{} {
	return l.MapP(1, mf)
}

func (l listLeaf) MapP(concurrency int, mf MapFunc) []interface{} {
	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}

	return l.mapInternal(limit, mf, 0)
}

func (l listLeaf) mapInternal(sem chan int, mf MapFunc, offset uint64) []interface{} {
	values := make([]interface{}, l.Len(), l.Len())

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	for idx := uint64(0); idx < l.Len(); idx++ {
		wg.Add(1)

		sem <- 1
		go func(idx uint64) {
			defer wg.Done()

			f := l.list[idx]
			v := f.Deref(l.cs)
			f.Release()

			c := mf(v, idx+offset)
			<-sem

			mu.Lock()
			values[idx] = c
			mu.Unlock()
		}(idx)
	}

	wg.Wait()
	return values
}

func (l listLeaf) getFuture(idx uint64) Future {
	return l.list[idx]
}

func (l listLeaf) Slice(start uint64, end uint64) List {
	return listFromFutures(l.list[start:end], l.cs)
}

func (l listLeaf) Set(idx uint64, v Value) List {
	b := make([]Future, len(l.list))
	copy(b, l.list)
	b[idx] = futureFromValue(v)
	return listFromFutures(b, l.cs)
}

func (l listLeaf) Append(v ...Value) List {
	return listFromFutures(append(l.list, valuesToFutures(v)...), l.cs)
}

func (l listLeaf) Insert(idx uint64, v ...Value) List {
	b := make([]Future, len(l.list)+len(v))
	copy(b, l.list[:idx])
	copy(b[idx:], valuesToFutures(v))
	copy(b[idx+uint64(len(v)):], l.list[idx:])
	return listFromFutures(b, l.cs)
}

func (l listLeaf) Remove(start uint64, end uint64) List {
	b := make([]Future, uint64(len(l.list))-(end-start))
	copy(b, l.list[:start])
	copy(b[start:], l.list[end:])
	return listFromFutures(b, l.cs)
}

func (l listLeaf) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

func (l listLeaf) Ref() ref.Ref {
	return ensureRef(l.ref, l)
}

// BUG 141
func (l listLeaf) Release() {
	for _, f := range l.list {
		f.Release()
	}
}

func (l listLeaf) Equals(other Value) bool {
	if other, ok := other.(listLeaf); ok {
		return l.Ref() == other.Ref()
	}
	return false
}

func (l listLeaf) Chunks() (futures []Future) {
	for _, f := range l.list {
		if f, ok := f.(*unresolvedFuture); ok {
			futures = append(futures, f)
		}
	}
	return
}

func (cl listLeaf) TypeRef() TypeRef {
	// TODO: The element type needs to be configurable.
	return MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
}
