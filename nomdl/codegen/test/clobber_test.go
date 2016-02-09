package test

import (
	"testing"

	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/stretchr/testify/assert"
)

func TestClobber(t *testing.T) {
	l := gen.NewListOfListOfBlob()
	assert.NotNil(t, l)
}
