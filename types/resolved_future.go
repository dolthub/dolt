package types

import "github.com/attic-labs/noms/dbg"

func FutureFromValue(v Value) Future {
	switch v.(type) {
	case resolvedFuture:
		dbg.Chk.Fail("Argh, we should never see non-pointer resolvedFuture")
	case *resolvedFuture:
		dbg.Chk.Fail("v must be non-future value")
	}
	return resolvedFuture{v}
}

type resolvedFuture struct {
	Value
}

func (rf resolvedFuture) Deref() (Value, error) {
	return rf.Value, nil
}
