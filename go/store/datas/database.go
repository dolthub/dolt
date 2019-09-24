// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package datas defines and implements the database layer used in Noms.
package datas

import (
	"context"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"io"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Database provides versioned storage for noms values. While Values can be
// directly read and written from a Database, it is generally more appropriate
// to read data by inspecting the Head of a Dataset and write new data by
// updating the Head of a Dataset via Commit() or similar. Particularly, new
// data is not guaranteed to be persistent until after a Commit (Delete,
// SetHead, or FastForward) operation completes.
// The Database API is stateful, meaning that calls to GetDataset() or
// Datasets() occurring after a call to Commit() (et al) will represent the
// result of the Commit().
type Database interface {
	// To implement types.ValueWriter, Database implementations provide
	// WriteValue(). WriteValue() writes v to this Database, though v is not
	// guaranteed to be be persistent until after a subsequent Commit(). The
	// return value is the Ref of v.
	// Written values won't be persisted until a commit-alike
	types.ValueReadWriter

	// Close must have no side-effects
	io.Closer

	// Datasets returns the root of the database which is a
	// Map<String, Ref<Commit>> where string is a datasetID.
	Datasets(ctx context.Context) (types.Map, error)

	// GetDataset returns a Dataset struct containing the current mapping of
	// datasetID in the above Datasets Map.
	GetDataset(ctx context.Context, datasetID string) (Dataset, error)

	// Rebase brings this Database's view of the world inline with upstream.
	Rebase(ctx context.Context) error

	// Commit updates the Commit that ds.ID() in this database points at. All
	// Values that have been written to this Database are guaranteed to be
	// persistent after Commit() returns.
	// The new Commit struct is constructed using v, opts.Parents, and
	// opts.Meta. If opts.Parents is the zero value (types.Set{}) then
	// the current head is used. If opts.Meta is the zero value
	// (types.Struct{}) then a fully initialized empty Struct is passed to
	// NewCommit.
	// The returned Dataset is always the newest snapshot, regardless of
	// success or failure, and Datasets() is updated to match backing storage
	// upon return as well. If the update cannot be performed, e.g., because
	// of a conflict, Commit returns an 'ErrMergeNeeded' error.
	Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error)

	// CommitValue updates the Commit that ds.ID() in this database points at.
	// All Values that have been written to this Database are guaranteed to be
	// persistent after Commit().
	// The new Commit struct is constructed using `v`, and the current Head of
	// `ds` as the lone Parent.
	// The returned Dataset is always the newest snapshot, regardless of
	// success or failure, and Datasets() is updated to match backing storage
	// upon return as well. If the update cannot be performed, e.g., because
	// of a conflict, Commit returns an 'ErrMergeNeeded' error.
	CommitValue(ctx context.Context, ds Dataset, v types.Value) (Dataset, error)

	// Delete removes the Dataset named ds.ID() from the map at the root of
	// the Database. The Dataset data is not necessarily cleaned up at this
	// time, but may be garbage collected in the future.
	// The returned Dataset is always the newest snapshot, regardless of
	// success or failure, and Datasets() is updated to match backing storage
	// upon return as well. If the update cannot be performed, e.g., because
	// of a conflict, Delete returns an 'ErrMergeNeeded' error.
	Delete(ctx context.Context, ds Dataset) (Dataset, error)

	// SetHead ignores any lineage constraints (e.g. the current Head being in
	// commit’s Parent set) and force-sets a mapping from datasetID: commit in
	// this database.
	// All Values that have been written to this Database are guaranteed to be
	// persistent after SetHead(). If the update cannot be performed, e.g.,
	// because another process moved the current Head out from under you,
	// error will be non-nil.
	// The newest snapshot of the Dataset is always returned, so the caller an
	// easily retry using the latest.
	// Regardless, Datasets() is updated to match backing storage upon return.
	SetHead(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error)

	// FastForward takes a types.Ref to a Commit object and makes it the new
	// Head of ds iff it is a descendant of the current Head. Intended to be
	// used e.g. after a call to Pull(). If the update cannot be performed,
	// e.g., because another process moved the current Head out from under
	// you, err will be non-nil.
	// The newest snapshot of the Dataset is always returned, so the caller
	// can easily retry using the latest.
	// Regardless, Datasets() is updated to match backing storage upon return.
	FastForward(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error)

	// Stats may return some kind of struct that reports statistics about the
	// ChunkStore that backs this Database instance. The type is
	// implementation-dependent, and impls may return nil
	Stats() interface{}

	// StatsSummary may return a string containing summarized statistics for
	// the ChunkStore that backs this Database. It must return "Unsupported"
	// if this operation is not supported.
	StatsSummary() string

	Flush(ctx context.Context) error

	// chunkStore returns the ChunkStore used to read and write
	// groups of values to the database efficiently. This interface is a low-
	// level detail of the database that should infrequently be needed by
	// clients.
	chunkStore() chunks.ChunkStore
}

func NewDatabase(cs chunks.ChunkStore) Database {
	return newDatabase(cs)
}

func CanUsePuller(db Database) bool {
	cs := db.chunkStore()
	_, ok := cs.(nbs.TableFileStore)

	return ok
}