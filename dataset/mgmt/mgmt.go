// Package mgmt implements management of datasets within a datastore.
package mgmt

import (
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func GetDatasets(ds datas.DataStore) SetOfDataset {
	if ds.Head().Equals(datas.EmptyCommit) {
		return NewSetOfDataset()
	}
	return SetOfDatasetFromVal(ds.Head().Value())
}

func CommitDatasets(ds datas.DataStore, datasets SetOfDataset) (datas.DataStore, bool) {
	newParents := datas.NewSetOfCommit().Insert(ds.Head())
	return ds.Commit(
		datas.NewCommit().SetParents(newParents.NomsValue()).SetValue(datasets.NomsValue()))
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

func GetDatasetHead(datasets SetOfDataset, datasetID string) types.Value {
	dataset := getDataset(datasets, datasetID)
	if dataset == nil {
		return nil
	}
	return dataset.Head()
}

func SetDatasetHead(datasets SetOfDataset, datasetID string, val types.Value) SetOfDataset {
	newDataset := NewDataset().SetId(types.NewString(datasetID)).SetHead(val)
	dataset := getDataset(datasets, datasetID)
	if dataset == nil {
		return datasets.Insert(newDataset)
	}
	return datasets.Remove(*dataset).Insert(newDataset)
}
