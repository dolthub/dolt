package dataset

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// Returns a datastore whose chunks are stored in rootStore, but whose root is associated with the specified dataset.
func NewDatasetDataStore(rootStore datas.DataStore, datasetID string) datas.DataStore {
	return datas.NewDataStore(rootStore, &datasetRootTracker{rootStore, datasetID})
}

type datasetDataStoreFlags struct {
	chunks.Flags
	datasetID *string
}

func DatasetDataFlags() datasetDataStoreFlags {
	return datasetDataStoreFlags{
		chunks.NewFlags(),
		flag.String("dataset-id", "", "dataset id to store data for"),
	}
}

func (f datasetDataStoreFlags) CreateStore() *datas.DataStore {
	if *f.datasetID == "" {
		return nil
	}
	cs := f.Flags.CreateStore()
	if cs == nil {
		return nil
	}

	// Blech, kinda sucks to typecast to RootTracker, but we know that all the implementations of ChunkStore that implement it.
	rootDataStore := datas.NewDataStore(cs, cs.(chunks.RootTracker))

	ds := NewDatasetDataStore(rootDataStore, *f.datasetID)
	return &ds
}

type datasetRootTracker struct {
	rootStore datas.DataStore
	datasetID string
}

func (rt *datasetRootTracker) Root() ref.Ref {
	dataset := GetDatasetRoot(GetDatasets(rt.rootStore), rt.datasetID)
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

	rt.rootStore = CommitDatasets(rt.rootStore, SetDatasetRoot(GetDatasets(rt.rootStore), rt.datasetID, datasetRoot))
	return true
}
