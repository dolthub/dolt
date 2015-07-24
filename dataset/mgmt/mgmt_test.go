package mgmt

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestGetDataset(t *testing.T) {
	assert := assert.New(t)
	ms := &chunks.MemoryStore{}
	ds := datas.NewDataStore(ms, ms)
	datasets := GetDatasets(ds)
	dataset := getDataset(datasets, "testdataset")
	assert.Nil(dataset)
	datasets = SetDatasetHeads(datasets, "testdataset", types.Int32(42))
	dataset = getDataset(datasets, "testdataset")
	assert.Equal("testdataset", dataset.Id().String())
}

func TestSetDatasetRoot(t *testing.T) {
	assert := assert.New(t)
	datasets := SetDatasetHeads(NewSetOfDataset(), "testdataset", types.Int32(42))
	assert.EqualValues(1, datasets.Len())
	assert.True(types.Int32(42).Equals(datasets.Any().Heads()))
	assert.True(types.Int32(42).Equals(GetDatasetHeads(datasets, "testdataset")))
}
