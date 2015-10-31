package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

func TestUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewTestStore()
	v := NewString("hello")
	r := WriteValue(v, cs)

	f := futureFromRef(r)
	v2 := f.Deref(cs)
	assert.Equal(1, cs.Reads)
	assert.True(v.Equals(v2))

	v3 := f.Deref(cs)
	assert.Equal(1, cs.Reads)
	assert.True(v2.Equals(v3))
}
