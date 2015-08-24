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
	r := types.WriteValue(types.Bool(true), cs)

	assert.Panics(t, func() { validateRefAsCommit(r, cs) })
}

func TestPull(t *testing.T) {
	assert := assert.New(t)

	puller := createTestDataset("puller")
	pullee := createTestDataset("pullee")

	// Give puller and pullee some initial shared context.
	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	ok := false
	pullee, ok = pullee.Commit(initialValue)
	assert.True(ok)
	puller, ok = puller.Commit(initialValue)
	assert.True(ok)

	// Add some new stuff to pullee.
	updatedValue := initialValue.Set(
		types.NewString("third"), types.NewList(types.Int32(3)))
	pullee, ok = pullee.Commit(updatedValue)
	assert.True(ok)

	// Add some more stuff, so that pullee isn't directly ahead of puller.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), types.NewList(types.Int32(4)))
	pullee, ok = pullee.Commit(updatedValue)
	assert.True(ok)

	refs := DiffHeadsByRef(puller.Head().Ref(), pullee.Head().Ref(), pullee.Store())
	CopyChunks(refs, pullee.Store(), puller.Store())
	puller, ok = SetNewHeads(pullee.Head().Ref(), puller)
	assert.True(ok)
	assert.True(pullee.Head().Equals(puller.Head()))
}

func TestPullFirstCommit(t *testing.T) {
	assert := assert.New(t)

	puller := createTestDataset("puller")
	pullee := createTestDataset("pullee")

	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	pullee, ok := pullee.Commit(initialValue)
	assert.True(ok)

	pullerHeadRef := func() ref.Ref {
		head, ok := puller.MaybeHead()
		if ok {
			return head.Ref()
		}
		return ref.Ref{}
	}()

	refs := DiffHeadsByRef(pullerHeadRef, pullee.Head().Ref(), pullee.Store())
	CopyChunks(refs, pullee.Store(), puller.Store())
	puller, ok = SetNewHeads(pullee.Head().Ref(), puller)
	assert.True(ok)
	assert.True(pullee.Head().Equals(puller.Head()))

}

func TestFailedCopyChunks(t *testing.T) {
	cs := &chunks.NopStore{}
	r := ref.Parse("sha1-0000000000000000000000000000000000000000")
	assert.Panics(t, func() { CopyChunks([]ref.Ref{r}, cs, cs) })
}
