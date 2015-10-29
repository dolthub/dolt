package newset

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestChunkSize3Depth4(t *testing.T) {
	configureAndTestChunkSize3Depth4(t, 81, 0)
	configureAndTestChunkSize3Depth4(t, 80, 1)
	configureAndTestChunkSize3Depth4(t, 41, 40)
	configureAndTestChunkSize3Depth4(t, 40, 41)
	configureAndTestChunkSize3Depth4(t, 1, 80)
	configureAndTestChunkSize3Depth4(t, 0, 81)
}

func configureAndTestChunkSize3Depth4(t *testing.T, numBuild, numPut int) {
	assert := assert.New(t)

	// Define the chunk size as 3 items, and aim for a tree with 4 layers of chunking (root, 3 internal layers, leaves), implying we need to add 3^4 = 81 items.
	chunkSize := 3
	assert.Equal(81, numBuild+numPut)
	set, refs := buildAndPutRefsInSet(t, newFixedSizeChunkerFactory(chunkSize), numBuild, numPut)

	// Top layer is the first chunked layer.
	first := set.root.(internal)
	assert.Equal(uint64(81), first.length())
	assert.Equal(refs[0], first.start())
	assert.Equal(chunkSize, len(first.children))

	for i := 0; i < chunkSize; i++ {
		// Second chunked layer:
		second := first.store.d[first.children[i].r].(internal)
		assert.Equal(uint64(27), second.length())
		assert.Equal(chunkSize, len(second.children))
		assert.Equal(refs[27*i], second.start())

		for j := 0; j < chunkSize; j++ {
			// Third chunked layer:
			third := first.store.d[second.children[j].r].(internal)
			assert.Equal(uint64(9), third.length())
			assert.Equal(chunkSize, len(third.children))
			assert.Equal(refs[27*i+9*j], third.start())

			for k := 0; k < chunkSize; k++ {
				// Fourth layer are the leaf nodes.
				fourth := first.store.d[third.children[k].r].(leaf)
				assert.Equal(uint64(3), fourth.length())
				assert.Equal(chunkSize, len(fourth.d))
				assert.Equal(refs[27*i+9*j+3*k], fourth.start())
				// Lastly, check the individual values of the leaf nodes.
				for m := 0; m < chunkSize; m++ {
					assert.Equal(refs[27*i+9*j+3*k+m], fourth.d[m])
				}
			}
		}
	}
}

func TestRealData(t *testing.T) {
	configureAndTestRealData(t, 5000, 0)
	configureAndTestRealData(t, 4999, 1)
	configureAndTestRealData(t, 4995, 5)
	// It would be nice to test these, but the Put implementation is currently way too slow:
	// configureAndTestRealData(t, 2501, 2499)
	// configureAndTestRealData(t, 2500, 2500)
	// configureAndTestRealData(t, 2499, 2501)
	// configureAndTestRealData(t, 1, 4999)
	// configureAndTestRealData(t, 0, 5000)
}

func configureAndTestRealData(t *testing.T, numBuild, numPut int) {
	assert := assert.New(t)
	set, _ := buildAndPutRefsInSet(t, newBuzChunker, numBuild, numPut)
	assert.Equal(expectedRealDataFmt, set.Fmt())
}

// Creates a new set with numBuild items added to the builder up front, building, then numPut items put into the set afterwards. Returns the set and the refs that were added to the set.
func buildAndPutRefsInSet(t *testing.T, newChunker chunkerFactory, numBuild, numPut int) (Set, ref.RefSlice) {
	assert := assert.New(t)

	store := newNodeStore()
	sb := NewSetBuilder(&store, newChunker)
	ator := newReferrator()
	var refs ref.RefSlice

	// Build and test the set has the correct size.
	for i := 0; i < numBuild; i++ {
		refs = append(refs, ator.Next())
		sb.AddItem(refs[i])
	}
	set := sb.Build()
	assert.Equal(uint64(numBuild), set.Len())

	// Add the rest of the items in random order and test the set has the correct size.
	for i := 0; i < numPut; i++ {
		refs = append(refs, ator.Next())
	}
	for _, i := range rand.Perm(numPut) {
		set = set.Put(refs[numBuild+i])
	}
	assert.Equal(uint64(numBuild+numPut), set.Len())

	// The set should have all refs, and not any others.
	for i := 0; i < len(refs); i++ {
		assert.True(set.Has(refs[i]))
	}
	assert.False(set.Has(ator.Next()))

	return set, refs
}
