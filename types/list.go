package types

import (
	"runtime"
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type List struct {
	values []Value
	t      TypeRef
	ref    *ref.Ref
}

var listTypeRef = MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))

func NewList(v ...Value) List {
	// Copy because Noms values are supposed to be immutable and Go allows v to be reused (thus mutable).
	values := make([]Value, len(v))
	copy(values, v)
	return newListNoCopy(values, listTypeRef)
}

func newListNoCopy(values []Value, t TypeRef) List {
	d.Chk.Equal(ListKind, t.Kind())
	return List{values, t, &ref.Ref{}}
}

func (l List) Len() uint64 {
	return uint64(len(l.values))
}

func (l List) Empty() bool {
	return l.Len() == uint64(0)
}

func (l List) Get(idx uint64) Value {
	return l.values[idx]
}

type listIterFunc func(v Value, index uint64) (stop bool)

func (l List) Iter(f listIterFunc) {
	for i, v := range l.values {
		if f(v, uint64(i)) {
			break
		}
	}
}

type listIterAllFunc func(v Value, index uint64)

func (l List) IterAll(f listIterAllFunc) {
	for i, v := range l.values {
		f(v, uint64(i))
	}
}

func (l List) IterAllP(concurrency int, f listIterAllFunc) {
	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}

	l.iterInternal(limit, f, 0)
}

func (l List) iterInternal(sem chan int, lf listIterAllFunc, offset uint64) {
	wg := sync.WaitGroup{}

	for idx := range l.values {
		wg.Add(1)

		sem <- 1
		go func(idx uint64) {
			defer wg.Done()
			lf(l.values[idx], idx+offset)
			<-sem
		}(uint64(idx))
	}

	wg.Wait()
}

type MapFunc func(v Value, index uint64) interface{}

func (l List) Map(mf MapFunc) []interface{} {
	return l.MapP(1, mf)
}

func (l List) MapP(concurrency int, mf MapFunc) []interface{} {
	var limit chan int
	if concurrency == 0 {
		limit = make(chan int, runtime.NumCPU())
	} else {
		limit = make(chan int, concurrency)
	}

	return l.mapInternal(limit, mf, 0)
}

func (l List) mapInternal(sem chan int, mf MapFunc, offset uint64) []interface{} {
	values := make([]interface{}, l.Len(), l.Len())
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	for idx := uint64(0); idx < l.Len(); idx++ {
		wg.Add(1)

		sem <- 1
		go func(idx uint64) {
			defer wg.Done()
			v := l.values[idx]
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

func (l List) Slice(start uint64, end uint64) List {
	return newListNoCopy(l.values[start:end], l.t)
}

func (l List) Set(idx uint64, v Value) List {
	assertType(l.elemType(), v)
	values := make([]Value, len(l.values))
	copy(values, l.values)
	values[idx] = v
	return newListNoCopy(values, l.t)
}

func (l List) Append(v ...Value) List {
	assertType(l.elemType(), v...)
	values := append(l.values, v...)
	return newListNoCopy(values, l.t)
}

func (l List) Insert(idx uint64, v ...Value) List {
	assertType(l.elemType(), v...)
	values := make([]Value, len(l.values)+len(v))
	copy(values, l.values[:idx])
	copy(values[idx:], v)
	copy(values[idx+uint64(len(v)):], l.values[idx:])
	return newListNoCopy(values, l.t)
}

func (l List) Remove(start uint64, end uint64) List {
	values := make([]Value, uint64(len(l.values))-(end-start))
	copy(values, l.values[:start])
	copy(values[start:], l.values[end:])
	return newListNoCopy(values, l.t)
}

func (l List) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

func (l List) Ref() ref.Ref {
	return EnsureRef(l.ref, l)
}

func (l List) Equals(other Value) bool {
	if other, ok := other.(List); ok {
		return l.Ref() == other.Ref()
	}
	return false
}

func (l List) Chunks() (chunks []ref.Ref) {
	for _, v := range l.values {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (l List) TypeRef() TypeRef {
	return l.t
}

func (l List) elemType() TypeRef {
	return l.t.Desc.(CompoundDesc).ElemTypes[0]
}
