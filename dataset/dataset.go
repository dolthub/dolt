package dataset

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset/mgmt"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// TODO: Could just literally use datastore and just refer to it as 'dataset'. Unsure which is better.
type Dataset struct {
	datas.DataStore
}

func NewDataset(rootStore datas.DataStore, datasetID string) Dataset {
	return Dataset{datas.NewDataStore(rootStore, &datasetRootTracker{rootStore, datasetID})}
}

func (ds *Dataset) Commit(newRoots datas.RootSet) Dataset {
	return Dataset{ds.DataStore.Commit(newRoots)}
}

type datasetFlags struct {
	chunks.Flags
	datasetID *string
}

func Flags() datasetFlags {
	return datasetFlags{
		chunks.NewFlags(),
		flag.String("dataset-id", "", "dataset id to store data for"),
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

	// Blech, kinda sucks to typecast to RootTracker, but we know that all the implementations of ChunkStore that implement it.
	rootDataStore := datas.NewDataStore(cs, cs.(chunks.RootTracker))

	ds := NewDataset(rootDataStore, *f.datasetID)
	return &ds
}

// TODO: Move to separate file
type datasetRootTracker struct {
	rootStore datas.DataStore
	datasetID string
}

func (rt *datasetRootTracker) Root() ref.Ref {
	dataset := mgmt.GetDatasetRoot(mgmt.GetDatasets(rt.rootStore), rt.datasetID)
	if dataset == nil {
		return ref.Ref{}
	} else {
		return dataset.Ref()
	}
}

func (rt *datasetRootTracker) UpdateRoot(current, last ref.Ref) bool {
	if last != rt.Root() {
		return false
	}

	// BUG 11: Sucks to have to read the dataset root here in order to commit.
	datasetRoot := types.MustReadValue(current, rt.rootStore)

	rt.rootStore = mgmt.CommitDatasets(rt.rootStore, mgmt.SetDatasetRoot(mgmt.GetDatasets(rt.rootStore), rt.datasetID, datasetRoot))
	return true
}
