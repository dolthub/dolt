// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import (
	"regexp"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

var DatasetRe = regexp.MustCompile(`[a-zA-Z0-9\-_/]+`)

var idRe = regexp.MustCompile("^" + DatasetRe.String() + "$")

type Dataset struct {
	store datas.Database
	id    string
}

func NewDataset(db datas.Database, datasetID string) Dataset {
	d.PanicIfTrue(!idRe.MatchString(datasetID), "Invalid dataset ID: %s", datasetID)
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

// HeadValue returns the Value field of the current head Commit.
func (ds *Dataset) HeadValue() types.Value {
	c := ds.Head()
	return c.Get(datas.ValueField)
}

// MaybeHeadValue returns the Value field of the current head Commit, if avaliable. If not it
// returns nil and 'false'.
func (ds *Dataset) MaybeHeadValue() (types.Value, bool) {
	c, ok := ds.Database().MaybeHead(ds.id)
	if !ok {
		return nil, false
	}
	return c.Get(datas.ValueField), true
}

// CommitValue updates the commit that a dataset points at. The new Commit struct is constructed using v and the current Head.
// If the update cannot be performed, e.g., because of a conflict, Commit returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) CommitValue(v types.Value) (Dataset, error) {
	return ds.Commit(v, CommitOptions{})
}

// Commit updates the commit that a dataset points at. The new Commit struct is constructed using `v` and `opts.Parents`.
// If `opts.Parents` is the zero value (`types.Set{}`) then the current head is used.
// If the update cannot be performed, e.g., because of a conflict, CommitWith returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value, opts CommitOptions) (Dataset, error) {
	parents := opts.Parents
	if (parents == types.Set{}) {
		parents = types.NewSet()
		if headRef, ok := ds.MaybeHeadRef(); ok {
			headRef.TargetValue(ds.Database()) // TODO: This is a hack to deconfuse the validation code, which doesn't hold onto validation state between commits.
			parents = parents.Insert(headRef)
		}
	}
	newCommit := datas.NewCommit(v, parents)
	store, err := ds.Database().Commit(ds.id, newCommit)
	return Dataset{store, ds.id}, err
}

func (ds *Dataset) Pull(sourceStore datas.Database, sourceRef types.Ref, concurrency int, progressCh chan datas.PullProgress) (Dataset, error) {
	sink := *ds

	sinkHeadRef := types.Ref{}
	if currentHeadRef, ok := sink.MaybeHeadRef(); ok {
		sinkHeadRef = currentHeadRef
	}

	if sourceRef == sinkHeadRef {
		return sink, nil
	}

	datas.Pull(sourceStore, sink.Database(), sourceRef, sinkHeadRef, concurrency, progressCh)
	err := datas.ErrOptimisticLockFailed
	for ; err == datas.ErrOptimisticLockFailed; sink, err = sink.setNewHead(sourceRef) {
	}

	return sink, err
}

func (ds *Dataset) validateRefAsCommit(r types.Ref) types.Struct {
	v := ds.store.ReadValue(r.TargetHash())

	if v == nil {
		panic(r.TargetHash().String() + " not found")
	}
	if !datas.IsCommitType(v.Type()) {
		panic("Not a commit: " + types.EncodedValue(v))
	}
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
	p := commit.Get(datas.ParentsField).(types.Set)
	return ds.Commit(commit.Get(datas.ValueField), CommitOptions{Parents: p})
}
