package dataset

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDatasetCommitTracker(t *testing.T) {
	assert := assert.New(t)
	datasetId1 := "testdataset"
	datasetId2 := "othertestdataset"
	ms := &chunks.MemoryStore{}

	datasetDs1 := NewDataset(datas.NewDataStore(ms, ms), datasetId1)
	datasetCommit1 := types.NewString("Commit value for " + datasetId1)
	datasetDs1 = datasetDs1.Commit(datas.NewCommitSet().Insert(
		datas.NewCommit().SetParents(
			types.NewSet()).SetValue(datasetCommit1)))

	datasetDs2 := NewDataset(datas.NewDataStore(ms, ms), datasetId2)
	datasetCommit2 := types.NewString("Commit value for " + datasetId2)
	datasetDs2 = datasetDs2.Commit(datas.NewCommitSet().Insert(
		datas.NewCommit().SetParents(
			types.NewSet()).SetValue(datasetCommit2)))

	assert.EqualValues(1, datasetDs2.Heads().Len())
	assert.EqualValues(1, datasetDs1.Heads().Len())
	assert.EqualValues(datasetCommit1, datasetDs1.Heads().Any().Value())
	assert.EqualValues(datasetCommit2, datasetDs2.Heads().Any().Value())
	assert.False(datasetDs2.Heads().Any().Value().Equals(datasetCommit1))
	assert.False(datasetDs1.Heads().Any().Value().Equals(datasetCommit2))

	assert.Equal("sha1-9b9fcfcd7e41ff727e6bea0edfa26f71def178a5", ms.Root().String())
}
