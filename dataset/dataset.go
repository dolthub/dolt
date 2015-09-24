package dataset

import (
	"flag"

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

// MaybeHead returns the current Head Commit of this Dataset, which contains the current root of the Dataset's value tree, if available. If not, it returns a new Commit and 'false'.
func (ds *Dataset) MaybeHead() (datas.Commit, bool) {
	sets := mgmt.GetDatasets(ds.store)
	head := mgmt.GetDatasetHead(sets, ds.id)
	if head == nil {
		return datas.NewCommit(), false
	}
	return datas.CommitFromVal(head), true
}

// Head returns the current head Commit, which contains the current root of the Dataset's value tree.
func (ds *Dataset) Head() datas.Commit {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return c
}

// Commit updates the commit that a dataset points at. The new Commit is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns 'false' and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value) (Dataset, bool) {
	p := datas.NewSetOfCommit()
	if head, ok := ds.MaybeHead(); ok {
		p = p.Insert(head)
	}
	return ds.CommitWithParents(v, p)
}

// CommitWithParents updates the commit that a dataset points at. The new Commit is constructed using v and p.
// If the update cannot be performed, e.g., because of a conflict, CommitWithParents returns 'false' and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) CommitWithParents(v types.Value, p datas.SetOfCommit) (Dataset, bool) {
	newCommit := datas.NewCommit().SetParents(p).SetValue(v)
	sets := mgmt.GetDatasets(ds.store)
	sets = mgmt.SetDatasetHead(sets, ds.id, newCommit.NomsValue())
	store, ok := mgmt.CommitDatasets(ds.store, sets)
	return Dataset{store, ds.id}, ok
}

func (ds *Dataset) Close() {
	ds.store.Close()
}

type datasetFlags struct {
	datas.Flags
	datasetID *string
}

func NewFlags() datasetFlags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) datasetFlags {
	return datasetFlags{
		datas.NewFlagsWithPrefix(prefix),
		flag.String(prefix+"ds", "", "dataset id to store data for"),
	}
}

func (f datasetFlags) CreateDataset() *Dataset {
	if *f.datasetID == "" {
		return nil
	}
	rootDS, ok := f.Flags.CreateDataStore()
	if !ok {
		return nil
	}

	ds := NewDataset(rootDS, *f.datasetID)
	return &ds
}
