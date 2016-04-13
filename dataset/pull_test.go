package dataset

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func createTestDataset(name string) Dataset {
	return NewDataset(datas.NewDataStore(chunks.NewTestStore()), name)
}

func TestValidateRef(t *testing.T) {
	ds := createTestDataset("test")
	r := ds.Store().WriteValue(types.Bool(true))

	assert.Panics(t, func() { ds.validateRefAsCommit(datas.NewRefOfCommit(r.TargetRef())) })
}

func NewList(ds Dataset, vs ...types.Value) types.RefBase {
	v := types.NewList(vs...)
	return ds.Store().WriteValue(v)
}

func NewMap(ds Dataset, vs ...types.Value) types.RefBase {
	v := types.NewMap(vs...)
	return ds.Store().WriteValue(v)
}

func NewSet(ds Dataset, vs ...types.Value) types.RefBase {
	v := types.NewSet(vs...)
	return ds.Store().WriteValue(v)
}

func pullTest(t *testing.T, topdown bool) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	// Give sink and source some initial shared context.
	sourceInitialValue := types.NewMap(
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Int32(2)))
	sinkInitialValue := types.NewMap(
		types.NewString("first"), NewList(sink),
		types.NewString("second"), NewList(sink, types.Int32(2)))

	var err error
	source, err = source.Commit(sourceInitialValue)
	assert.NoError(err)
	sink, err = sink.Commit(sinkInitialValue)
	assert.NoError(err)

	// Add some new stuff to source.
	updatedValue := sourceInitialValue.Set(
		types.NewString("third"), NewList(source, types.Int32(3)))
	source, err = source.Commit(updatedValue)
	assert.NoError(err)

	// Add some more stuff, so that source isn't directly ahead of sink.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), NewList(source, types.Int32(4)))
	source, err = source.Commit(updatedValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Store(), datas.NewRefOfCommit(source.Head().Ref()), 1, topdown)
	assert.NoError(err)
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

	sourceInitialValue := types.NewMap(
		types.NewString("first"), NewList(source),
		types.NewString("second"), NewList(source, types.Int32(2)))

	NewList(sink)
	NewList(sink, types.Int32(2))

	source, err := source.Commit(sourceInitialValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Store(), datas.NewRefOfCommit(source.Head().Ref()), 1, topdown)
	assert.NoError(err)
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

	sourceInitialValue := types.NewList(
		types.NewList(NewList(source)),
		types.NewSet(NewSet(source)),
		types.NewMap(NewMap(source), NewMap(source)))

	source, err := source.Commit(sourceInitialValue)
	assert.NoError(err)

	sink, err = sink.pull(source.Store(), datas.NewRefOfCommit(source.Head().Ref()), 1, topdown)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullDeepRefTopDown(t *testing.T) {
	pullDeepRef(t, true)
}

func TestPullDeepRefExclude(t *testing.T) {
	pullDeepRef(t, false)
}
