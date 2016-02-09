package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	assert := assert.New(t)

	st := NewPackage([]Type{}, []ref.Ref{})
	typ := st.Type()
	assert.Equal(PackageKind, typ.Kind())
}
