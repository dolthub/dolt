package newset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func TestNumMatches(t *testing.T) {
	assert := assert.New(t)
	chunker := newBuzChunker()

	numMatches := 0
	for i := 0; i < 1000; i++ {
		if chunker.Add(getRef(i)) {
			numMatches++
		}
	}

	// 20 was experimentally determined by calling Add 1000 times.
	assert.Equal(20, numMatches)
}

func TestThing(t *testing.T) {
	assert := assert.New(t)
	// This ref has been experimentally determined to be immediately chunked.
	r := ref.Parse("sha1-00000000000000000000000000000000000f422f")
	assert.True(newBuzChunker().Add(r))
}

func getRef(i int) ref.Ref {
	return types.Int32(i).Ref()
}
