package sync

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func createTestDataset(name string) dataset.Dataset {
	t := &chunks.TestStore{}
	return dataset.NewDataset(datas.NewDataStore(t), name)

}

func TestValidateRef(t *testing.T) {
	cs := &chunks.TestStore{}
	r, err := types.WriteValue(types.Bool(true), cs)
	assert.NoError(t, err)

	assert.Panics(t, func() { validateRefAsSetOfCommit(r, cs) })
}

func TestPull(t *testing.T) {
	assert := assert.New(t)

	puller := createTestDataset("puller")
	pullee := createTestDataset("pullee")

	commitValue := func(v types.Value, ds dataset.Dataset) dataset.Dataset {
		return ds.Commit(
			datas.NewSetOfCommit().Insert(
				datas.NewCommit().SetParents(ds.Heads().NomsValue()).SetValue(v)))
	}

	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	pullee = commitValue(initialValue, pullee)
	puller = commitValue(initialValue, puller)

	updatedValue := initialValue.Set(
		types.NewString("third"), types.NewList(types.Int32(1)))

	pullee = commitValue(updatedValue, pullee)

	refs := DiffHeadsByRef(puller.Heads().Ref(), pullee.Heads().Ref(), pullee)
	CopyChunks(refs, pullee, puller)
	puller = SetNewHeads(pullee.Heads().Ref(), puller)
	assert.Equal(pullee.Heads().Ref(), puller.Heads().Ref())
	assert.True(pullee.Heads().Equals(puller.Heads()))

}

func TestPullFirstCommit(t *testing.T) {
	assert := assert.New(t)

	puller := createTestDataset("puller")
	pullee := createTestDataset("pullee")

	commitValue := func(v types.Value, ds dataset.Dataset) dataset.Dataset {
		return ds.Commit(
			datas.NewSetOfCommit().Insert(
				datas.NewCommit().SetParents(ds.Heads().NomsValue()).SetValue(v)))
	}

	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	pullee = commitValue(initialValue, pullee)

	refs := DiffHeadsByRef(puller.Heads().Ref(), pullee.Heads().Ref(), pullee)
	CopyChunks(refs, pullee, puller)
	puller = SetNewHeads(pullee.Heads().Ref(), puller)
	assert.Equal(pullee.Heads().Ref(), puller.Heads().Ref())
	assert.True(pullee.Heads().Equals(puller.Heads()))

}

func TestFailedCopyChunks(t *testing.T) {
	cs := &chunks.NopStore{}
	r := ref.MustParse("sha1-0000000000000000000000000000000000000000")
	assert.Panics(t, func() { CopyChunks([]ref.Ref{r}, cs, cs) })
}
