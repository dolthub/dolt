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

func NewList(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewList(ds.Store(), vs...)
	r := types.WriteValue(v, ds.store)
	return types.NewRef(r)
}

func NewMap(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewMap(ds.Store(), vs...)
	r := types.WriteValue(v, ds.store)
	return types.NewRef(r)
}

func NewSet(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewSet(ds.Store(), vs...)
	r := types.WriteValue(v, ds.store)
	return types.NewRef(r)
}

func pullTest(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	// Give sink and source some initial shared context.
	sourceInitialValue := types.NewMap(source.Store(),
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Int32(2)))
	sinkInitialValue := types.NewMap(sink.Store(),
		types.NewString("first"), NewList(sink),
		types.NewString("second"), NewList(sink, types.Int32(2)))

	ok := false
	source, ok = source.Commit(sourceInitialValue)
	assert.True(ok)
	sink, ok = sink.Commit(sinkInitialValue)
	assert.True(ok)

	// Add some new stuff to source.
	updatedValue := sourceInitialValue.Set(
		types.NewString("third"), NewList(source, types.Int32(3)))
	source, ok = source.Commit(updatedValue)
	assert.True(ok)

	// Add some more stuff, so that source isn't directly ahead of sink.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), NewList(source, types.Int32(4)))
	source, ok = source.Commit(updatedValue)
	assert.True(ok)

	sink = sink.pull(source, 1, topdown, ref.Ref{})
	assert.True(ok)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullTopDown(t *testing.T) {
	pullTest(t, true)
}

func TestPullExclude(t *testing.T) {
	pullTest(t, false)
}

func pullFirstCommit(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewMap(source.Store(),
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Int32(2)))

	NewList(sink)
	NewList(sink, types.Int32(2))

	source, ok := source.Commit(sourceInitialValue)
	assert.True(ok)

	sink = sink.pull(source, 1, topdown, ref.Ref{})
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullFirstCommitTopDown(t *testing.T) {
	pullFirstCommit(t, true)
}

func TestPullFirstCommitExclude(t *testing.T) {
	pullFirstCommit(t, false)
}

func pullDeepRef(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewList(source.Store(),
		types.NewList(source.Store(), NewList(source)),
		types.NewSet(source.Store(), NewSet(source)),
		types.NewMap(source.Store(), NewMap(source), NewMap(source)))

	source, ok := source.Commit(sourceInitialValue)
	assert.True(ok)

	sink = sink.pull(source, 1, topdown, ref.Ref{})
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullDeepRefTopDown(t *testing.T) {
	pullDeepRef(t, true)
}

func TestPullDeepRefExclude(t *testing.T) {
	pullDeepRef(t, false)
}
