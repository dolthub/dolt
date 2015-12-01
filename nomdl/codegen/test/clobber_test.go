package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestClobber(t *testing.T) {
	cs := chunks.NewMemoryStore()
	l := gen.NewListOfListOfBlob(cs)
	assert.NotNil(t, l)
}
