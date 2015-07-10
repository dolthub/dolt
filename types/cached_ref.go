package types

import (
	"github.com/attic-labs/noms/ref"
)

type cachedRef ref.Ref

func (cr *cachedRef) Ref(v Value) ref.Ref {
	if ref.Ref(*cr) == (ref.Ref{}) {
		*cr = cachedRef(getRef(v))
	}
	return ref.Ref(*cr)
}
