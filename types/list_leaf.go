package types

import (
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type listLeaf struct {
	values []Value
	t      *Type
	ref    *ref.Ref
}

func newListLeaf(t *Type, v ...Value) List {
	d.Chk.Equal(ListKind, t.Kind())
	return listLeaf{v, t, &ref.Ref{}}
}

func (l listLeaf) Len() uint64 {
	return uint64(len(l.values))
}

func (l listLeaf) Empty() bool {
	return l.Len() == uint64(0)
}

func (l listLeaf) Get(idx uint64) Value {
	return l.values[idx]
}

func (l listLeaf) Iter(f listIterFunc) {
	for i, v := range l.values {
		if f(v, uint64(i)) {
			break
		}
	}
}

func (l listLeaf) IterAll(f listIterAllFunc) {
	for i, v := range l.values {
		f(v, uint64(i))
	}
}

func (l listLeaf) iterInternal(sem chan int, lf listIterAllFunc, offset uint64) {
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

func (l listLeaf) Filter(cb listFilterCallback) List {
	data := []Value{}
	for i, v := range l.values {
		if cb(v, uint64(i)) {
			data = append(data, v)
		}
	}

	return NewTypedList(l.t, data...)
}

func (l listLeaf) Map(mf MapFunc) []interface{} {
	results := []interface{}{}
	for i, v := range l.values {
		res := mf(v, uint64(i))
		results = append(results, res)
	}
	return results
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

func (l listLeaf) Slice(start uint64, end uint64) List {
	return NewTypedList(l.t, l.values[start:end]...)
}

func (l listLeaf) Set(idx uint64, v Value) List {
	assertType(l.elemType(), v)
	values := make([]Value, len(l.values))
	copy(values, l.values)
	values[idx] = v
	return NewTypedList(l.t, values...)
}

func (l listLeaf) Append(v ...Value) List {
	assertType(l.elemType(), v...)
	values := append(l.values, v...)
	return NewTypedList(l.t, values...)
}

func (l listLeaf) Insert(idx uint64, v ...Value) List {
	assertType(l.elemType(), v...)
	values := make([]Value, len(l.values)+len(v))
	copy(values, l.values[:idx])
	copy(values[idx:], v)
	copy(values[idx+uint64(len(v)):], l.values[idx:])
	return NewTypedList(l.t, values...)
}

func (l listLeaf) Remove(start uint64, end uint64) List {
	values := make([]Value, uint64(len(l.values))-(end-start))
	copy(values, l.values[:start])
	copy(values[start:], l.values[end:])
	return NewTypedList(l.t, values...)
}

func (l listLeaf) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

func (l listLeaf) Ref() ref.Ref {
	return EnsureRef(l.ref, l)
}

func (l listLeaf) Equals(other Value) bool {
	if other, ok := other.(List); ok {
		return l.Ref() == other.Ref()
	}
	return false
}

func (l listLeaf) Chunks() (chunks []Ref) {
	for _, v := range l.values {
		chunks = append(chunks, v.Chunks()...)
	}
	return
}

func (l listLeaf) ChildValues() []Value {
	return append([]Value{}, l.values...)
}

func (l listLeaf) Type() *Type {
	return l.t
}

func (l listLeaf) elemType() *Type {
	return l.t.Desc.(CompoundDesc).ElemTypes[0]
}
