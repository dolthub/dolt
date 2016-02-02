package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestClobber(t *testing.T) {
	l := gen.NewListOfListOfBlob()
	assert.NotNil(t, l)
}
