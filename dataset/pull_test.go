package dataset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func createTestDataset(name string) Dataset {
	return NewDataset(datas.NewDataStore(chunks.NewTestStore()), name)
}

func TestValidateRef(t *testing.T) {
	ds := createTestDataset("test")
	r := types.WriteValue(types.Bool(true), ds.Store())

	assert.Panics(t, func() { ds.validateRefAsCommit(r) })
}

func TestPull(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	// Give sink and source some initial shared context.
	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	ok := false
	source, ok = source.Commit(initialValue)
	assert.True(ok)
	sink, ok = sink.Commit(initialValue)
	assert.True(ok)

	// Add some new stuff to source.
	updatedValue := initialValue.Set(
		types.NewString("third"), types.NewList(types.Int32(3)))
	source, ok = source.Commit(updatedValue)
	assert.True(ok)

	// Add some more stuff, so that source isn't directly ahead of sink.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), types.NewList(types.Int32(4)))
	source, ok = source.Commit(updatedValue)
	assert.True(ok)

	sink = sink.Pull(source, 1)
	assert.True(ok)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullFirstCommit(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	source, ok := source.Commit(initialValue)
	assert.True(ok)

	sink = sink.Pull(source, 1)
	assert.True(source.Head().Equals(sink.Head()))

}

func TestFailedCopyChunks(t *testing.T) {
	ds := createTestDataset("test")
	r := ref.Parse("sha1-0000000000000000000000000000000000000000")
	assert.Panics(t, func() { ds.Store().CopyReachableChunksP(r, ref.Ref{}, ds.Store(), 1) })
}
