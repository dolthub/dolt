package sync

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func createTestDataset(name string) dataset.Dataset {
	t := &chunks.TestStore{}
	return dataset.NewDataset(datas.NewDataStore(t), name)
}

func TestValidateRef(t *testing.T) {
	cs := &chunks.TestStore{}
	r := types.WriteValue(types.Bool(true), cs)

	assert.Panics(t, func() { validateRefAsCommit(r, cs) })
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

	CopyReachableChunksP(source.Head().Ref(), sink.Head().Ref(), source.Store(), sink.Store(), 1)
	sink, ok = SetNewHead(source.Head().Ref(), sink)
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

	sinkHeadRef := func() ref.Ref {
		head, ok := sink.MaybeHead()
		if ok {
			return head.Ref()
		}
		return ref.Ref{}
	}()

	CopyReachableChunksP(source.Head().Ref(), sinkHeadRef, source.Store(), sink.Store(), 1)
	CopyReachableChunksP(source.Head().Ref(), sinkHeadRef, source.Store(), sink.Store(), 1)
	sink, ok = SetNewHead(source.Head().Ref(), sink)
	assert.True(ok)
	assert.True(source.Head().Equals(sink.Head()))

}

func TestFailedCopyChunks(t *testing.T) {
	cs := &chunks.MemoryStore{}
	r := ref.Parse("sha1-0000000000000000000000000000000000000000")
	assert.Panics(t, func() { CopyReachableChunksP(r, ref.Ref{}, cs, cs, 1) })
}
