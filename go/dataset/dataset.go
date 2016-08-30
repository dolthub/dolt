// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package dataset implements the dataset layer of Noms that sits above the database.
package dataset

import (
	"regexp"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

var DatasetRe = regexp.MustCompile(`[a-zA-Z0-9\-_/]+`)
var DatasetFullRe = regexp.MustCompile("^" + DatasetRe.String() + "$")

type Dataset struct {
	store datas.Database
	id    string
}

func NewDataset(db datas.Database, datasetID string) Dataset {
	d.PanicIfTrue(!DatasetFullRe.MatchString(datasetID), "Invalid dataset ID: %s", datasetID)
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

// MaybeHeadValue returns the Value field of the current head Commit, if available. If not it
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

// Commit updates the commit that a dataset points at. The new Commit struct is constructed using `v`, `opts.Parents`, and `opts.Meta`.
// If `opts.Parents` is the zero value (`types.Set{}`) then the current head is used.
// If `opts.Meta is the zero value (`types.Struct{}`) then a fully initialized empty Struct is passed to NewCommit.
// If the update cannot be performed, e.g., because of a conflict, CommitWith returns an 'ErrMergeNeeded' error and the current snapshot of the dataset so that the client can merge the changes and try again.
func (ds *Dataset) Commit(v types.Value, opts CommitOptions) (Dataset, error) {
	parents := opts.Parents
	if (parents == types.Set{}) {
		parents = types.NewSet()
		if headRef, ok := ds.MaybeHeadRef(); ok {
			parents = parents.Insert(headRef)
		}
	}

	meta := opts.Meta
	// Ideally, would like to do 'if meta == types.Struct{}' but types.Struct is not comparable in Go
	// since it contains a slice.
	if meta.Type() == nil && len(meta.ChildValues()) == 0 {
		meta = types.EmptyStruct
	}
	newCommit := datas.NewCommit(v, parents, meta)
	store, err := ds.Database().Commit(ds.id, newCommit)
	return Dataset{store, ds.id}, err
}

// Pull objects that descend from sourceRef in srcDB into sinkDB, using at most the given degree of concurrency. Progress will be reported over progressCh as the algorithm works. Objects that are already present in ds will not be pulled over.
func (ds *Dataset) Pull(sourceDB datas.Database, sourceRef types.Ref, concurrency int, progressCh chan datas.PullProgress) {
	sinkHeadRef := types.Ref{}
	if currentHeadRef, ok := ds.MaybeHeadRef(); ok {
		sinkHeadRef = currentHeadRef
	}
	datas.Pull(sourceDB, ds.Database(), sourceRef, sinkHeadRef, concurrency, progressCh)
}

// FastForward takes a types.Ref to a Commit object and makes it the new Head of ds iff it is a descendant of the current Head. Intended to be used e.g. after a call to Pull(). If the update cannot be performed, e.g., because another process moved the current Head out from under you, err will be non-nil. The newest snapshot of the Dataset is always returned, so the caller an easily retry using the latest.
func (ds *Dataset) FastForward(newHeadRef types.Ref) (sink Dataset, err error) {
	sink = *ds
	if currentHeadRef, ok := sink.MaybeHeadRef(); ok && newHeadRef == currentHeadRef {
		return
	} else if newHeadRef.Height() <= currentHeadRef.Height() {
		return sink, datas.ErrMergeNeeded
	}

	for err = datas.ErrOptimisticLockFailed; err == datas.ErrOptimisticLockFailed; sink, err = sink.commitNewHead(newHeadRef) {
	}
	return
}

// SetHead takes a types.Ref to a Commit object and makes it the new Head of ds. Intended to be used e.g. when rewinding in ds' Commit history. If the update cannot be performed, e.g., because the state of ds.Database() changed out from under you, err will be non-nil. The newest snapshot of the Dataset is always returned, so the caller an easily retry using the latest.
func (ds *Dataset) SetHead(newHeadRef types.Ref) (sink Dataset, err error) {
	sink = *ds
	if currentHeadRef, ok := sink.MaybeHeadRef(); ok && newHeadRef == currentHeadRef {
		return
	}

	commit := sink.validateRefAsCommit(newHeadRef)
	store, err := sink.Database().SetHead(sink.id, commit)
	return Dataset{store, sink.id}, err
}

func (ds *Dataset) validateRefAsCommit(r types.Ref) types.Struct {
	v := ds.Database().ReadValue(r.TargetHash())

	if v == nil {
		panic(r.TargetHash().String() + " not found")
	}
	if !datas.IsCommitType(v.Type()) {
		panic("Not a commit: " + types.EncodedValue(v))
	}
	return v.(types.Struct)
}

// commitNewHead attempts to make the object pointed to by newHeadRef the new Head of ds. First, it checks that the object exists in ds and validates that it decodes to the correct type of value. Next, it attempts to commit the object to ds.Database(). This may fail if, for instance, the Head of ds has been changed by another goroutine or process. In the event that the commit fails, the error from Database().Commit() is returned along with a new Dataset that's at it's proper, current Head. The caller should take any necessary corrective action and try again using this new Dataset.
func (ds *Dataset) commitNewHead(newHeadRef types.Ref) (Dataset, error) {
	commit := ds.validateRefAsCommit(newHeadRef)
	store, err := ds.Database().Commit(ds.id, commit)
	return Dataset{store, ds.id}, err
}
