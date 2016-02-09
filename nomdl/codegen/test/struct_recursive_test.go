package test

import (
	"testing"

	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
	"github.com/stretchr/testify/assert"
)

func TestStructRecursiveChildren(t *testing.T) {
	assert := assert.New(t)

	root := gen.TreeDef{
		Children: []gen.TreeDef{
			gen.TreeDef{},
			gen.TreeDef{
				Children: []gen.TreeDef{
					gen.TreeDef{},
				},
			},
		},
	}.New()

	assert.Equal(uint64(2), root.Children().Len())
	assert.Equal(uint64(0), root.Children().Get(0).Children().Len())
	assert.Equal(uint64(1), root.Children().Get(1).Children().Len())
}
