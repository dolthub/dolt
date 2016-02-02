package dataset

import (
	"flag"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type Dataset struct {
	store datas.DataStore
	id    string
}

func NewDataset(store datas.DataStore, datasetID string) Dataset {
	d.Exp.NotEmpty(datasetID, "Cannot create an unnamed Dataset.")
	return Dataset{store, datasetID}
}

func (ds *Dataset) Store() datas.DataStore {
	return ds.store
}

func (ds *Dataset) ID() string {
	return ds.id
}

// MaybeHead returns the current Head Commit of this Dataset, which contains the current root of the Dataset's value tree, if available. If not, it returns a new Commit and 'false'.
func (ds *Dataset) MaybeHead() (datas.Commit, bool) {
	return ds.Store().MaybeHead(ds.id)
}

// Head returns the current head Commit, which contains the current root of the Dataset's value tree.
func (ds *Dataset) Head() datas.Commit {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return c
}

// Commit updates the commit that a dataset points at. The new Commit is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value) (Dataset, error) {
	p := datas.NewSetOfRefOfCommit()
	if head, ok := ds.MaybeHead(); ok {
		p = p.Insert(datas.NewRefOfCommit(head.Ref()))
	}
	return ds.CommitWithParents(v, p)
}

// CommitWithParents updates the commit that a dataset points at. The new Commit is constructed using v and p.
// If the update cannot be performed, e.g., because of a conflict, CommitWithParents returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) CommitWithParents(v types.Value, p datas.SetOfRefOfCommit) (Dataset, error) {
	newCommit := datas.NewCommit().SetParents(p).SetValue(v)
	store, err := ds.Store().Commit(ds.id, newCommit)
	return Dataset{store, ds.id}, err
}

func (ds *Dataset) Pull(sourceStore datas.DataStore, sourceRef ref.Ref, concurrency int) (Dataset, error) {
	_, topDown := ds.Store().(*datas.LocalDataStore)
	return ds.pull(sourceStore, sourceRef, concurrency, topDown)
}

func (ds *Dataset) pull(source datas.DataStore, sourceRef ref.Ref, concurrency int, topDown bool) (Dataset, error) {
	sink := *ds

	sinkHeadRef := ref.Ref{}
	if currentHead, ok := sink.MaybeHead(); ok {
		sinkHeadRef = currentHead.Ref()
	}

	if sourceRef == sinkHeadRef {
		return sink, nil
	}

	if topDown {
		source.CopyMissingChunksP(sourceRef, sink.Store(), concurrency)
	} else {
		source.CopyReachableChunksP(sourceRef, sinkHeadRef, sink.Store(), concurrency)
	}
	err := datas.ErrOptimisticLockFailed
	for ; err == datas.ErrOptimisticLockFailed; sink, err = sink.SetNewHead(sourceRef) {
	}

	return sink, err
}

func (ds *Dataset) validateRefAsCommit(r ref.Ref) datas.Commit {
	v := types.ReadValue(r, ds.store)

	d.Exp.NotNil(v, "%v cannot be found", r)

	// TODO: Replace this weird recover stuff below once we have a way to determine if a Value is an instance of a custom struct type. BUG #133
	defer func() {
		if r := recover(); r != nil {
			d.Exp.Fail("Not a Commit:", "%+v", v)
		}
	}()

	return v.(datas.Commit)
}

// SetNewHead takes the Ref of the desired new Head of ds, the chunk for which should already exist in the Dataset. It validates that the Ref points to an existing chunk that decodes to the correct type of value and then commits it to ds, returning a new Dataset with newHeadRef set and ok set to true. In the event that the commit fails, ok is set to false and a new up-to-date Dataset is returned WITHOUT newHeadRef in it. The caller should take any necessary corrective action and try again using this new Dataset.
func (ds *Dataset) SetNewHead(newHeadRef ref.Ref) (Dataset, error) {
	commit := ds.validateRefAsCommit(newHeadRef)
	return ds.CommitWithParents(commit.Value(), commit.Parents())
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
