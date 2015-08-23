package dataset

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset/mgmt"
	"github.com/attic-labs/noms/types"
)

type Dataset struct {
	store datas.DataStore
	id    string
}

func NewDataset(store datas.DataStore, datasetID string) Dataset {
	return Dataset{store, datasetID}
}

func (ds *Dataset) Store() datas.DataStore {
	return ds.store
}

func (ds *Dataset) MaybeHead() (datas.Commit, bool) {
	sets := mgmt.GetDatasets(ds.store)
	head := mgmt.GetDatasetHead(sets, ds.id)
	if head == nil {
		return datas.NewCommit(), false
	}
	return datas.CommitFromVal(head), true
}

func (ds *Dataset) Head() datas.Commit {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "Dataset %s does not exist", ds.id)
	return c
}

func (ds *Dataset) HeadAsSet() types.Set {
	commit, ok := ds.MaybeHead()
	commits := datas.NewSetOfCommit()
	if ok {
		commits = commits.Insert(commit)
	}
	return commits.NomsValue()
}

// Commit updates the commit that a dataset points at.
// If the update cannot be performed, e.g., because of a
// conflict, the current snapshot of the dataset is
// returned so that the client can merge the changes and
// try again.
func (ds *Dataset) Commit(newCommit datas.Commit) (Dataset, bool) {
	sets := mgmt.GetDatasets(ds.store)
	sets = mgmt.SetDatasetHead(sets, ds.id, newCommit.NomsValue())
	store, ok := mgmt.CommitDatasets(ds.store, sets)
	return Dataset{store, ds.id}, ok
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
