package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestStructRecursiveChildren(t *testing.T) {
	assert := assert.New(t)

	root := TreeDef{
		Children: []TreeDef{
			TreeDef{},
			TreeDef{
				Children: []TreeDef{
					TreeDef{},
				},
			},
		},
	}.New()

	assert.Equal(uint64(2), root.Children().Len())
	assert.Equal(uint64(0), root.Children().Get(0).Children().Len())
	assert.Equal(uint64(1), root.Children().Get(1).Children().Len())
}
