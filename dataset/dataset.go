package dataset

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

type Dataset struct {
	store datas.Database
	id    string
}

func NewDataset(store datas.Database, datasetID string) Dataset {
	d.Exp.NotEmpty(datasetID, "Cannot create an unnamed Dataset.")
	return Dataset{store, datasetID}
}

func (ds *Dataset) Store() datas.Database {
	return ds.store
}

func (ds *Dataset) ID() string {
	return ds.id
}

// MaybeHead returns the current Head Commit of this Dataset, which contains the current root of the Dataset's value tree, if available. If not, it returns a new Commit and 'false'.
func (ds *Dataset) MaybeHead() (types.Struct, bool) {
	return ds.Store().MaybeHead(ds.id)
}

func (ds *Dataset) MaybeHeadHash() (types.Ref, bool) {
	return ds.Store().MaybeHeadRef(ds.id)
}

// Head returns the current head Commit, which contains the current root of the Dataset's value tree.
func (ds *Dataset) Head() types.Struct {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return c
}

func (ds *Dataset) HeadHash() types.Ref {
	r, ok := ds.MaybeHeadHash()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return r
}

// Commit updates the commit that a dataset points at. The new Commit is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value) (Dataset, error) {
	p := types.NewSet()
	if headRef, ok := ds.MaybeHeadHash(); ok {
		headRef.TargetValue(ds.Store()) // TODO: This is a hack to deconfuse the validation code, which doesn't hold onto validation state between commits.
		p = p.Insert(headRef)
	}
	return ds.CommitWithParents(v, p)
}

// CommitWithParents updates the commit that a dataset points at. The new Commit is constructed using v and p.
// If the update cannot be performed, e.g., because of a conflict, CommitWithParents returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) CommitWithParents(v types.Value, p types.Set) (Dataset, error) {
	newCommit := datas.NewCommit().Set(datas.ParentsField, p).Set(datas.ValueField, v)
	store, err := ds.Store().Commit(ds.id, newCommit)
	return Dataset{store, ds.id}, err
}

func (ds *Dataset) Pull(sourceStore datas.Database, sourceRef types.Ref, concurrency int) (Dataset, error) {
	_, topDown := ds.Store().(*datas.LocalDatabase)
	return ds.pull(sourceStore, sourceRef, concurrency, topDown)
}

func (ds *Dataset) pull(source datas.Database, sourceRef types.Ref, concurrency int, topDown bool) (Dataset, error) {
	sink := *ds

	sinkHeadRef := types.Ref{}
	if currentHeadRef, ok := sink.MaybeHeadHash(); ok {
		sinkHeadRef = currentHeadRef
	}

	if sourceRef == sinkHeadRef {
		return sink, nil
	}

	if topDown {
		datas.CopyMissingChunksP(source, sink.Store().(*datas.LocalDatabase), sourceRef, concurrency)
	} else {
		datas.CopyReachableChunksP(source, sink.Store(), sourceRef, sinkHeadRef, concurrency)
	}

	err := datas.ErrOptimisticLockFailed
	for ; err == datas.ErrOptimisticLockFailed; sink, err = sink.setNewHead(sourceRef) {
	}

	return sink, err
}

func (ds *Dataset) validateRefAsCommit(r types.Ref) types.Struct {
	v := ds.store.ReadValue(r.TargetHash())

	d.Exp.NotNil(v, "%v cannot be found", r)
	d.Exp.True(v.Type().Equals(datas.NewCommit().Type()), "Not a Commit: %+v", v)
	return v.(types.Struct)
}

// setNewHead takes the Ref of the desired new Head of ds, the chunk for which should already exist
// in the Dataset. It validates that the Ref points to an existing chunk that decodes to the correct
// type of value and then commits it to ds, returning a new Dataset with newHeadRef set and ok set
// to true. In the event that the commit fails, ok is set to false and a new up-to-date Dataset is
// returned WITHOUT newHeadRef in it. The caller should take any necessary corrective action and try
// again using this new Dataset.
func (ds *Dataset) setNewHead(newHeadRef types.Ref) (Dataset, error) {
	commit := ds.validateRefAsCommit(newHeadRef)
	return ds.CommitWithParents(commit.Get(datas.ValueField), commit.Get(datas.ParentsField).(types.Set))
}
