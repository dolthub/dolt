// Package mgmt implements management of datasets within a datastore.
package mgmt

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func GetDatasets(ds datas.DataStore) SetOfDataset {
	if ds.Heads().Empty() {
		return NewSetOfDataset()
	} else {
		// BUG 13: We don't ever want to branch the datasets database. Currently we can't avoid that, but we should change DataStore::Commit() to support that mode of operation.
		d.Chk.EqualValues(1, ds.Heads().Len())
		return SetOfDatasetFromVal(ds.Heads().Any().Value())
	}
}

func CommitDatasets(ds datas.DataStore, datasets SetOfDataset) datas.DataStore {
	return ds.Commit(datas.NewSetOfCommit().Insert(
		datas.NewCommit().SetParents(
			ds.Heads().NomsValue()).SetValue(
			datasets.NomsValue())))
}

func getDataset(datasets SetOfDataset, datasetID string) (r *Dataset) {
	datasets.Iter(func(dataset Dataset) (stop bool) {
		if dataset.Id().String() == datasetID {
			r = &dataset
			stop = true
		}
		return
	})
	return
}

func GetDatasetHeads(datasets SetOfDataset, datasetID string) types.Value {
	dataset := getDataset(datasets, datasetID)
	if dataset == nil {
		return nil
	}
	return dataset.Heads()
}

func SetDatasetHeads(datasets SetOfDataset, datasetID string, val types.Value) SetOfDataset {
	newDataset := NewDataset().SetId(types.NewString(datasetID)).SetHeads(val)
	dataset := getDataset(datasets, datasetID)
	if dataset == nil {
		return datasets.Insert(newDataset)
	}
	return datasets.Remove(*dataset).Insert(newDataset)
}
