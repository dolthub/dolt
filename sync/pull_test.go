package sync

import (
	"io/ioutil"
	"os"
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

	commitValue := func(v types.Value, ds dataset.Dataset) (dataset.Dataset, bool) {
		return ds.Commit(datas.NewCommit().SetParents(ds.HeadAsSet()).SetValue(v))
	}

	// Give puller and pullee some initial shared context.
	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	ok := false
	pullee, ok = commitValue(initialValue, pullee)
	assert.True(ok)
	puller, ok = commitValue(initialValue, puller)
	assert.True(ok)

	// Add some new stuff to pullee.
	updatedValue := initialValue.Set(
		types.NewString("third"), types.NewList(types.Int32(3)))
	pullee, ok = commitValue(updatedValue, pullee)
	assert.True(ok)

	// Add some more stuff, so that pullee isn't directly ahead of puller.
	updatedValue = updatedValue.Set(
		types.NewString("fourth"), types.NewList(types.Int32(4)))
	pullee, ok = commitValue(updatedValue, pullee)
	assert.True(ok)

	refs := DiffHeadsByRef(puller.Head().Ref(), pullee.Head().Ref(), pullee)
	CopyChunks(refs, pullee, puller)
	puller, ok = SetNewHeads(pullee.Head().Ref(), puller)
	assert.True(ok)
	assert.True(pullee.Head().Equals(puller.Head()))
}

func TestPullFirstCommit(t *testing.T) {
	assert := assert.New(t)

	puller := createTestDataset("puller")
	pullee := createTestDataset("pullee")

	commitValue := func(v types.Value, ds dataset.Dataset) (dataset.Dataset, bool) {
		return ds.Commit(datas.NewCommit().SetParents(ds.HeadAsSet()).SetValue(v))
	}

	initialValue := types.NewMap(
		types.NewString("first"), types.NewList(),
		types.NewString("second"), types.NewList(types.Int32(2)))

	pullee, ok := commitValue(initialValue, pullee)
	assert.True(ok)

	refs := DiffHeadsByRef(puller.Head().Ref(), pullee.Head().Ref(), pullee)
	CopyChunks(refs, pullee, puller)
	puller, ok = SetNewHeads(pullee.Head().Ref(), puller)
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

	dir := func() string {
		d, err := ioutil.TempDir(os.TempDir(), "")
		assert.NoError(err)
		return d
	}

	fs1 := chunks.NewFileStore(dir(), "root")
	fs2 := chunks.NewFileStore(dir(), "root")

	// Populate a filestore with a ton of chunks. Child structs are always out of line.
	set := types.NewSet()
	// On aa@'s mbp, values higher than 256 crash inside CopyChunks() due to "too many open files". Using a much larger value for theoretical other machines that tolerate larger amounts of open files.
	for i := int32(0); i < 5000; i++ {
		item := types.NewSet(types.Int64(int32(i)))
		r := types.WriteValue(item, fs1)
		set = set.Insert(types.Ref{r})
	}

	source := dataset.NewDataset(datas.NewDataStore(fs1), "source")
	sink := dataset.NewDataset(datas.NewDataStore(fs2), "sink")
	assert.True(source.Heads().Equals(sink.Heads()))

	source = source.Commit(datas.NewSetOfCommit().Insert(datas.NewCommit().SetParents(source.Heads().NomsValue()).SetValue(set)))
	assert.False(source.Heads().Equals(sink.Heads()))

	newHead := source.Heads().Ref()
	refs := DiffHeadsByRef(sink.Heads().Ref(), newHead, source)
	CopyChunks(refs, source, sink)
	sink = SetNewHeads(newHead, sink)
	assert.True(source.Heads().Equals(sink.Heads()))
}
