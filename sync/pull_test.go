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
	puller, ok = SetNewHead(pullee.Head().Ref(), puller)
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
	puller, ok = SetNewHead(pullee.Head().Ref(), puller)
	assert.True(ok)
	assert.True(pullee.Head().Equals(puller.Head()))

}

func TestFailedCopyChunks(t *testing.T) {
	cs := &chunks.NopStore{}
	r := ref.Parse("sha1-0000000000000000000000000000000000000000")
	assert.Panics(t, func() { CopyChunks([]ref.Ref{r}, cs, cs) })
}

func TestTonsOChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	// This is a stress test of pulling a large number of chunks. See https://github.com/attic-labs/noms/issues/213 for an example of an issue this would have caught.
	assert := assert.New(t)

	ms1 := &chunks.MemoryStore{}
	ms2 := &chunks.MemoryStore{}

	// Populate a filestore with a ton of chunks. Child structs are always out of line.
	set := types.NewSet()
	// On aa@'s mbp, values higher than 256 crash inside CopyChunks() due to "too many open files". Using a much larger value for theoretical other machines that tolerate larger amounts of open files.
	for i := int32(0); i < 5000; i++ {
		item := types.NewSet(types.Int64(int32(i)))
		r := types.WriteValue(item, ms1)
		set = set.Insert(types.Ref{r})
	}

	source := dataset.NewDataset(datas.NewDataStore(ms1), "source")
	sink := dataset.NewDataset(datas.NewDataStore(ms2), "sink")
	s := types.NewString("dummy")
	source, _ = source.Commit(s)
	sink, _ = sink.Commit(s)
	assert.True(source.Head().Equals(sink.Head()))

	source, ok := source.Commit(set)
	assert.True(ok)
	assert.False(source.Head().Equals(sink.Head()))

	newHead := source.Head().Ref()
	refs := DiffHeadsByRef(sink.Head().Ref(), newHead, source.Store())
	CopyChunks(refs, source.Store(), sink.Store())
	sink, ok = SetNewHead(newHead, sink)
	assert.True(ok)
	assert.True(source.Head().Equals(sink.Head()))
}
