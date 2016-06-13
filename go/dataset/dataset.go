// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import (
	"regexp"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

var idRe = regexp.MustCompile(`^[a-zA-Z0-9\-_/]+$`)

type Dataset struct {
	store datas.Database
	id    string
}

func NewDataset(db datas.Database, datasetID string) Dataset {
	d.Exp.True(idRe.MatchString(datasetID), "Invalid dataset ID: %s", datasetID)
	return Dataset{db, datasetID}
}

func (ds *Dataset) Database() datas.Database {
	return ds.store
}

func (ds *Dataset) ID() string {
	return ds.id
}

// MaybeHead returns the current Head Commit of this Dataset, which contains the current root of the Dataset's value tree, if available. If not, it returns a new Commit and 'false'.
func (ds *Dataset) MaybeHead() (types.Struct, bool) {
	return ds.Database().MaybeHead(ds.id)
}

func (ds *Dataset) MaybeHeadRef() (types.Ref, bool) {
	return ds.Database().MaybeHeadRef(ds.id)
}

// Head returns the current head Commit, which contains the current root of the Dataset's value tree.
func (ds *Dataset) Head() types.Struct {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return c
}

func (ds *Dataset) HeadRef() types.Ref {
	r, ok := ds.MaybeHeadRef()
	d.Chk.True(ok, "Dataset \"%s\" does not exist", ds.id)
	return r
}

// Commit updates the commit that a dataset points at. The new Commit is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value) (Dataset, error) {
	p := types.NewSet()
	if headRef, ok := ds.MaybeHeadRef(); ok {
		headRef.TargetValue(ds.Database()) // TODO: This is a hack to deconfuse the validation code, which doesn't hold onto validation state between commits.
		p = p.Insert(headRef)
	}
	return ds.CommitWithParents(v, p)
}

// CommitWithParents updates the commit that a dataset points at. The new Commit is constructed using v and p.
// If the update cannot be performed, e.g., because of a conflict, CommitWithParents returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) CommitWithParents(v types.Value, p types.Set) (Dataset, error) {
	newCommit := datas.NewCommit().Set(datas.ParentsField, p).Set(datas.ValueField, v)
	store, err := ds.Database().Commit(ds.id, newCommit)
	return Dataset{store, ds.id}, err
}

func (ds *Dataset) Pull(sourceStore datas.Database, sourceRef types.Ref, concurrency int) (Dataset, error) {
	return ds.pull(sourceStore, sourceRef, concurrency)
}

func (ds *Dataset) pull(source datas.Database, sourceRef types.Ref, concurrency int) (Dataset, error) {
	sink := *ds

	sinkHeadRef := types.Ref{}
	if currentHeadRef, ok := sink.MaybeHeadRef(); ok {
		sinkHeadRef = currentHeadRef
	}

	if sourceRef == sinkHeadRef {
		return sink, nil
	}

	datas.Pull(source, sink.Database(), sourceRef, sinkHeadRef, concurrency)
	err := datas.ErrOptimisticLockFailed
	for ; err == datas.ErrOptimisticLockFailed; sink, err = sink.setNewHead(sourceRef) {
	}

	return sink, err
}

func (ds *Dataset) validateRefAsCommit(r types.Ref) types.Struct {
	v := ds.store.ReadValue(r.TargetHash())

	d.Exp.True(v != nil, "%v cannot be found", r)
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
