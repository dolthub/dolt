package dataset

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset/mgmt"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type Dataset struct {
	datas.DataStore
}

func NewDataset(parentStore datas.DataStore, datasetID string) Dataset {
	return Dataset{datas.NewDataStoreWithRootTracker(parentStore, &datasetRootTracker{parentStore, datasetID})}
}

func (ds *Dataset) Commit(newCommit datas.Commit) (Dataset, bool) {
	store, ok := ds.DataStore.Commit(newCommit)
	return Dataset{store}, ok
}

type datasetFlags struct {
	chunks.Flags
	datasetID *string
}

func NewFlags() datasetFlags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) datasetFlags {
	return datasetFlags{
		chunks.NewFlagsWithPrefix(prefix),
		flag.String(prefix+"ds", "", "dataset id to store data for"),
	}
}

func (f datasetFlags) CreateDataset() *Dataset {
	if *f.datasetID == "" {
		return nil
	}
	cs := f.Flags.CreateStore()
	if cs == nil {
		return nil
	}

	rootDS := datas.NewDataStore(cs)
	ds := NewDataset(rootDS, *f.datasetID)
	return &ds
}

// TODO: Move to separate file
type datasetRootTracker struct {
	parentStore datas.DataStore
	datasetID   string
}

func (rt *datasetRootTracker) Root() ref.Ref {
	dataset := mgmt.GetDatasetHead(mgmt.GetDatasets(rt.parentStore), rt.datasetID)
	if dataset == nil {
		return ref.Ref{}
	}
	return dataset.Ref()
}

func (rt *datasetRootTracker) UpdateRoot(current, last ref.Ref) bool {
	datasetCommit := types.ReadValue(current, rt.parentStore)
	newDatasets := mgmt.SetDatasetHead(mgmt.GetDatasets(rt.parentStore), rt.datasetID, datasetCommit)
	ok := false
	rt.parentStore, ok = mgmt.CommitDatasets(rt.parentStore, newDatasets)
	return ok
}
