package types

import "github.com/attic-labs/noms/ref"

func FutureFromRef(ref ref.Ref, res Resolver) Future {
	return &unresolvedFuture{ref: ref, res: res}
}

type unresolvedFuture struct {
	Value
	res Resolver
	ref ref.Ref
	val Value
}

func (f *unresolvedFuture) Deref() (Value, error) {
	if f.val != nil {
		return f.val, nil
	}

	val, err := f.res(f.ref)
	if err != nil {
		return nil, err
	}

	f.val = val
	return f.val, nil
}

func (f *unresolvedFuture) Ref() ref.Ref {
	return f.ref
}

func (f *unresolvedFuture) Equals(other Value) bool {
	return f.ref == other.Ref()
}
