package dataset

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDatasetRootTracker(t *testing.T) {
	assert := assert.New(t)
	datasetId1 := "testdataset"
	datasetId2 := "othertestdataset"
	ms := &chunks.MemoryStore{}
	rootDs := datas.NewDataStore(ms, ms)

	datasetDs1 := NewDataset(rootDs, datasetId1)
	datasetRoot1 := types.NewString("Root value for " + datasetId1)
	datasetDs1 = datasetDs1.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			types.NewSet()).SetValue(datasetRoot1)))

	datasetDs2 := NewDataset(rootDs, datasetId2)
	datasetRoot2 := types.NewString("Root value for " + datasetId2)
	datasetDs2 = datasetDs2.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			types.NewSet()).SetValue(datasetRoot2)))

	assert.EqualValues(1, datasetDs2.Roots().Len())
	assert.EqualValues(1, datasetDs1.Roots().Len())
	assert.EqualValues(datasetRoot1, datasetDs1.Roots().Any().Value())
	assert.EqualValues(datasetRoot2, datasetDs2.Roots().Any().Value())
	assert.False(datasetDs2.Roots().Any().Value().Equals(datasetRoot1))
	assert.False(datasetDs1.Roots().Any().Value().Equals(datasetRoot2))
}
