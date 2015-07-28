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

func NewDataset(parentStore datas.DataStore, datasetID string) Dataset {
	return Dataset{datas.NewDataStore(parentStore, &datasetRootTracker{parentStore, datasetID})}
}

func (ds *Dataset) Commit(newCommits datas.SetOfCommit) Dataset {
	return Dataset{ds.DataStore.Commit(newCommits)}
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
		flag.String("ds", "", "dataset id to store data for"),
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

	// Blech, kinda sucks to typecast to RootTracker, but we know that all the implementations of ChunkStore implement it.
	commitDataStore := datas.NewDataStore(cs, cs.(chunks.RootTracker))

	ds := NewDataset(commitDataStore, *f.datasetID)
	return &ds
}

// TODO: Move to separate file
type datasetRootTracker struct {
	parentStore datas.DataStore
	datasetID   string
}

func (rt *datasetRootTracker) Root() ref.Ref {
	dataset := mgmt.GetDatasetHeads(mgmt.GetDatasets(rt.parentStore), rt.datasetID)
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

	datasetCommit := types.MustReadValue(current, rt.parentStore)
	rt.parentStore = mgmt.CommitDatasets(rt.parentStore, mgmt.SetDatasetHeads(mgmt.GetDatasets(rt.parentStore), rt.datasetID, datasetCommit))
	return true
}
